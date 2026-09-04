// Engine/src/Modules/BF_DAG/dag.odin
//
// Frame_DAG and the DAG compiler. The compiler reads the System_Registry
// the engine assembled during scheduler_build(), materializes one DAG
// node per system, computes the dependency bitmask (stage ordering +
// same-stage access conflicts + explicit before/after edges), and
// pre-computes everything the worker loop needs (depth, dependents,
// owner_worker, cost estimate).
//
// Ported from DagScheduler/Dag.odin (Ymir engine). The only changes:
//   * System_Registry.systems now uses System_Entry (System_ID + name +
//     proc(rawptr) callback) instead of the Ymir ECS.System struct.
//   * Frame_Budget
//     in Bifrost (they were scattered across Dag.odin in Ymir).
package BF_DAG

import "../../Core"
import "core:mem"
import "core:time"

Frame_DAG :: struct {
	task_ids:           []Task,
	// Dependencies -> node
	dependencies_start: []i32,
	dependencies_count: []i32,
	dependencies_flat:  []i32,
	//  Node -> dependencies
	dependents_start:   []i32,
	dependents_count:   []i32,
	dependents_flat:    []i32,
	// Topological metadata
	topo_order:         []i32,
	depth:              []i32,
	// Scheduler hints
	preferred_worker:   []i16,
	// Cost metadata
	cost_estimate:      []f32,
	critical_cost:      []f32,
	generation:         u64,
}

DAG_Edge :: struct {
	before: i32,
	after:  i32,
}

Frame_Budget :: struct {
	max_ms:         f32, // e.g. 16.6ms / 33.3ms
	used_ms:        f32,
	remaining_ms:   f32,
	frame_start_ns: u64,
}

compile_frame_dag :: proc(
	registry: ^System_Registry,
	worker_count: int,
	allocator: mem.Allocator,
) -> Frame_DAG {
	n := len(registry.systems)
	dag: Frame_DAG
	dag_init(&dag, n, worker_count, allocator)
	//* Systems -> scheduler tasks
	for i in 0 ..< n {
		sys := &registry.systems[i]
		dag.task_ids[i] = Task {
			id         = sys.id,
			name       = sys.name,
			fn         = sys.callback,
			kind       = .System,
			read_mask  = sys.info.read_mask,
			write_mask = sys.info.write_mask,
		}
		// Until runtime profiling exists, every system gets a neutral base cost.
		dag.cost_estimate[i] = 1.0
	}
	//* Build explicit dependency edges.
	// An edge is always: before -> after
	// There are no dependency bitmasks anymore.
	edges: [dynamic]DAG_Edge
	//* Stage ordering.
	// This preserves the current semantic contract:
	// earlier stage -> later stage
	// We can relax this into a hazard-driven graph later, but keeping the
	// stage barrier here makes this migration safe and keeps it explicit.
	for after in 0 ..< n {
		after_stage := cast(int)registry.systems[after].info.stage
		for before in 0 ..< n {
			if before == after do continue
			before_stage := cast(int)registry.systems[before].info.stage
			if before_stage < after_stage {
				append(&edges, DAG_Edge{before = i32(before), after = i32(after)})
			}
		}
	}
	//* Same-stage resource conflict.
	// Keeping deterministic registry ordering for now: this is too conservative.
	// TODO: In a later pass I can replace this with a proper read/write hazard graph which allows more same-stage parallelism
	for after in 0 ..< n {
		for before in 0 ..< after {
			if registry.systems[before].info.stage != registry.systems[after].info.stage do continue
			if task_conflict(&dag.task_ids[before], &dag.task_ids[after]) {
				append(&edges, DAG_Edge{before = i32(before), after = i32(after)})
			}
		}
	}
	//* Explicit registry dependencies.
	for dep in registry.dependencies {
		before := dag_find_system_index_by_id(registry, dep.before)
		after := dag_find_system_index_by_id(registry, dep.after)
		if before < 0 || after < 0 do continue
		if before == after do continue
		append(&edges, DAG_Edge{before = i32(before), after = i32(after)})
	}
	//* Remove duplicate edges
	// The stage rules and explicit dependencies can describe the same edge. Duplicates
	// would otherwise increment the dependency counter twice and deadlock the node.
	deduplicated: [dynamic]DAG_Edge
	for edge in edges {
		duplicate := false
		for existing in deduplicated {
			if existing.before == edge.before && existing.after == edge.after {
				duplicate = true
				break
			}
		}
		if !duplicate {append(&deduplicated, edge)}
	}
	delete(edges)
	edges = deduplicated
	//* Build both adjacency directions.
	// dependencies: node -> nodes that must finish before it
	// dependents: node -> nodes that become eligible after it completes
	dag_build_dependency_graph(&dag, edges[:], allocator)
	//* Validate and produce deterministic topological order.
	if !dag_topological_sort(
		&dag,
		allocator,
	) {panic("[BF_DAG] cycle detected in compile system graph")}
	//* Compute graph metadata
	dag_compute_depths(&dag)
	dag_compute_critical_cost(&dag)
	dag_assign_worker_affinity_compile(&dag, worker_count)

	delete(edges)
	return dag
}

//* SYSTEM_LOOKUP
dag_find_system_index_by_id :: proc(registry: ^System_Registry, id: Core.System_ID) -> int {
	for i in 0 ..< len(registry.systems) {
		if registry.systems[i].id == id do return i
	}
	return -1
}

//* BUILD DEPENDENCY + DEPENDENT CSR ARRAYS
dag_build_dependency_graph :: proc(dag: ^Frame_DAG, edges: []DAG_Edge, allocator: mem.Allocator) {
	n := len(dag.task_ids)
	// Count incoming and outgoing edges.
	// dependencies_count[node] = # of predecessors
	// dependents_count[node] = # of successors
	for i in 0 ..< n {
		dag.dependencies_start[i] = 0
		dag.dependencies_count[i] = 0
		dag.dependents_flat[i] = 0
		dag.dependents_count[i] = 0
	}
	for edge in edges {
		before := int(edge.before)
		after := int(edge.after)
		dag.dependencies_count[after] += 1
		dag.dependents_count[before] += 1
	}
	// Prefix sums -> CSR offsets
	dependency_total: int = 0
	dependent_total: int = 0
	for i in 0 ..< n {
		dag.dependencies_start[i] = i32(dependency_total)
		dependency_total += int(dag.dependencies_count[i])
		dag.dependents_start[i] = i32(dependent_total)
		dependent_total += int(dag.dependents_count[i])
	}
	// Allocate flat adjecency arrays.
	delete(dag.dependencies_flat, allocator)
	delete(dag.dependents_flat, allocator)
	dag.dependencies_flat = make([]i32, dependency_total, allocator)
	dag.dependents_flat = make([]i32, dependent_total, allocator)
	if n == 0 do return
	// Cursors begin at each node's CSR range.
	dependency_cursor := make([]i32, n, allocator)
	dependent_cursor := make([]i32, n, allocator)
	for i in 0 ..< n {
		dependency_cursor[i] = dag.dependencies_start[i]
		dependent_cursor[i] = dag.dependents_start[i]
	}

	// Fill both adjecency directions. Edge: before -> after
	// Becomes: depencies[after] += before
	// 			dependents[before] += after
	for edge in edges {
		before := int(edge.before)
		after := int(edge.after)
		dep_idx := int(dependency_cursor[after])
		dag.dependencies_flat[dep_idx] = i32(before)
		dependency_cursor[before] += 1

		dependent_index := int(dependent_cursor[before])
		dag.dependents_flat[dependent_index] = i32(after)
		dependent_cursor[before] += 1
	}
	delete(dependency_cursor, allocator)
	delete(dependent_cursor, allocator)
}

//* TOPOLOGICAL SORT
// Kahn's algorithm.   Produces: dag.topo_order
// Returns false when the graph contains a cycle.
dag_topological_sort :: proc(dag: ^Frame_DAG, allocator: mem.Allocator) -> bool {
	n := len(dag.task_ids)

	delete(dag.topo_order, allocator)
	if n == 0 {
		dag.topo_order = make([]i32, 0, allocator)
		return true
	}
	// Working copy of indegree.
	indegree := make([]i32, n, allocator)
	// kahn queue.
	queue := make([dynamic]i32, 0, n, allocator) 
	// Initial indegree is simply the # of dependencies.
	for i in 0 ..< n {
		indegree[i] = dag.dependencies_count[i]
		if indegree[i] == 0 {
			append(&queue, i32(i))
		}
	}
	// Allocate the final topological order directly.
	dag.topo_order = make([]i32, n, allocator)
	head := 0
	topo_count := 0 
	for head < len(queue) {
		node := int(queue[head])
		head += 1
		
		dag.topo_order[topo_count] = i32(node)
		topo_count += 1
		start := int(dag.dependents_count[node])
		count := int(dag.dependents_count[node])

		for j in 0 ..< count {
			dependent := dag.dependents_flat[start + j]
			indegree[dependent] -= 1
			if indegree[dependent] == 0 {
				append(&queue, i32(dependent))
			}
		}
	}
	delete(indegree, allocator)
	delete(queue)

	// A DAG containing a cycle cannot produce a complete topo ordering.
	if topo_count != n{
		delete(dag.topo_order, allocator)
		dag.topo_order = nil
		return false
	}
	return true
}
//* DEPTH
// depth(root) = 0, depth(node) = max(depth(parent) + 1)
dag_compute_depths :: proc(dag: ^Frame_DAG) {
	n := len(dag.task_ids)
	for i in 0 ..< n {
		dag.depth[i] = 0
	}
	for order_index in 0 ..< len(dag.topo_order) {
		node := int(dag.topo_order[order_index])
		start := int(dag.dependents_start[node])
		count := int(dag.dependents_count[node])
		for j in 0 ..< count {
			dependent := int(dag.dependents_flat[start + j])
			next_depth := dag.depth[node] + 1
			if dag.depth[dependent] < next_depth {
				dag.depth[dependent] = next_depth
			}
		}
	}
}
//* CRITICAL PATH COST
// critical_cost[node] = cost(node) + max(critical_cost[dependent])
// computed backwards through the topological order.
dag_compute_critical_cost :: proc(dag: ^Frame_DAG) {
	n := len(dag.task_ids)
	for i in 0 ..< n {
		dag.critical_cost[i] = dag.cost_estimate[i]
	}
	if len(dag.topo_order) == 0 do return
	// Reverse topo order.
	for order_index := len(dag.topo_order) - 1; order_index >= 0; order_index -= 1 {
		node := int(dag.topo_order[order_index])
		start := int(dag.dependents_start[node])
		count := int(dag.dependents_count[node])
		for j in 0 ..< count {
			dependent := int(dag.dependents_flat[start + j])
			candidate := dag.cost_estimate[node] + dag.critical_cost[dependent]
			if candidate > dag.critical_cost[node] {dag.critical_cost[node] = candidate}
		}
	}
}
//* WORKER HINT
// This is only an initial placement hint.
// The scheduler remains responsible for actual execution, stealing and locality.
dag_assign_worker_affinity_compile :: proc(dag: ^Frame_DAG, worker_count: int) {
	if worker_count <= 0 {
		for i in 0 ..< len(dag.preferred_worker) {dag.preferred_worker[i] = 0}
		return
	}
	for i in 0 ..< len(dag.task_ids) {dag.preferred_worker[i] = i16(i % worker_count)}
}

dag_init :: proc(dag: ^Frame_DAG, n: int, _worker_count: int, allocator: mem.Allocator) {
	dag.task_ids = make([]Task, n, allocator)
	dag.preferred_worker = make([]i16, n, allocator)
	dag.depth = make([]i32, n, allocator)
	dag.cost_estimate = make([]f32, n, allocator)
	dag.critical_cost = make([]f32, n, allocator)
	dag.topo_order = make([]i32, 0, allocator)
	dag.dependencies_start = make([]i32, n, allocator)
	dag.dependencies_count = make([]i32, n, allocator)
	dag.dependencies_flat = make([]i32, 0, allocator)
	dag.dependents_start = make([]i32, n, allocator)
	dag.dependents_count = make([]i32, n, allocator)
	dag.dependents_flat = make([]i32, 0, allocator)
}

dag_clear :: proc(dag: ^Frame_DAG, allocator: mem.Allocator) {
	delete(dag.task_ids, allocator)
	delete(dag.dependencies_start, allocator)
	delete(dag.dependencies_count, allocator)
	delete(dag.dependencies_flat, allocator)
	delete(dag.dependents_start, allocator)
	delete(dag.dependents_count, allocator)
	delete(dag.dependents_flat, allocator)
	delete(dag.topo_order, allocator)
	delete(dag.depth, allocator)
	delete(dag.preferred_worker, allocator)
	delete(dag.cost_estimate, allocator)
	delete(dag.critical_cost, allocator)
	dag^ = Frame_DAG{}
}

get_time_ns :: proc() -> u64 {
	return u64(time.now()._nsec)
}

frame_budget_begin :: proc(runtime: ^Scheduler_Runtime, max_ms: f32) {
	runtime.frame_budget.max_ms = max_ms
	runtime.frame_budget.used_ms = 0
	runtime.frame_budget.remaining_ms = max_ms

	runtime.frame_budget.frame_start_ns = get_time_ns()
}

frame_budget_update :: proc(runtime: ^Scheduler_Runtime) {
	now := get_time_ns()

	elapsed_ms := f32(now - runtime.frame_budget.frame_start_ns) / f32(1_000_000.0)

	runtime.frame_budget.used_ms = elapsed_ms
	runtime.frame_budget.remaining_ms = runtime.frame_budget.max_ms - elapsed_ms
}
