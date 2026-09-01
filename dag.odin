// Engine/src/Modules/BF_DAG/dag.odin
//
// Frame_DAG and the DAG compiler. The compiler reads the System_Registry
// the engine assembled during scheduler_build(), materializes one DAG
// node per system, computes the dependency bitmask (stage ordering +
// same-stage access conflicts + explicit before/after edges), and
// pre-computes everything the worker loop needs (depth, dependents,
// owner_worker, cache_group, cost estimate).
//
// Ported from DagScheduler/Dag.odin (Ymir engine). The only changes:
//   * System_Registry.systems now uses System_Entry (System_ID + name +
//     proc(rawptr) callback) instead of the Ymir ECS.System struct.
//   * Frame_Budget / Frame_Island / Frame_Dependency_Storage live here
//     in Bifrost (they were scattered across Dag.odin in Ymir).
package BF_DAG

import "core:fmt"
import "core:mem"
import "core:time"
import "../../Core"

Frame_DAG :: struct {
	task_ids:         []Task,
	dep_masks:        []u64,
	self_bits:        []u64,
	preferred_worker: []i16,
	preferred_numa:   []i16,
	cache_group:      []u32,
	depth:            []i32,
	cost_estimate:    []f32,
	dependents_start: []i32,
	dependents_count: []i32,
	dependents_flat:  []i32,
	owner_worker:     []i16,
}

Frame_Dependency_Storage :: struct {
	flat:  []i32,
	start: []i32,
	count: []i32,
}

Frame_Island :: struct {
	priority:  f32,
	pred_cost: f32,
}

Frame_Budget :: struct {
	max_ms:         f32, // e.g. 16.6ms / 33.3ms
	used_ms:        f32,
	remaining_ms:   f32,
	frame_start_ns: u64,
}

SIMD_WIDTH            :: 4
INVALID_CACHE_GROUP   :: u32(0xFFFFFFFF)

SIMD_Batch :: struct {
	nodes: [SIMD_WIDTH]int,
	count: int,
}

// --- NEW procs
dag_assign_numa :: proc(dag: ^Frame_DAG) {
	n := len(dag.task_ids)

	// simple alternating NUMA heuristic (no graph traversal)
	for i in 0 ..< n {dag.preferred_numa[i] = i16(i % 2)}
}

compile_frame_dag :: proc(
	registry: ^System_Registry,
	worker_count: int,
	allocator: mem.Allocator,
) -> Frame_DAG {
	n := len(registry.systems)

	dag: Frame_DAG
	dag_init(&dag, n, worker_count, allocator)

	// ---------------------------------------------------------
	// 1. Fill SOA task array + self bits
	// ---------------------------------------------------------
	for i in 0 ..< n {
		sys := &registry.systems[i]

		dag.task_ids[i] = Task {
			id         = sys.id,
			name       = sys.name,
			fn         = sys.callback,
			read_mask  = sys.info.read_mask,
			write_mask = sys.info.write_mask,
		}
		dag.self_bits[i] = u64(1) << u64(i)
	}

	// ---------------------------------------------------------
	// 2. Build dependency bitmasks (ONLY SOURCE OF TRUTH)
	// ---------------------------------------------------------
	// Stage ordering: every task in an earlier stage must complete first.
	for i in 0 ..< n {
		for j in 0 ..< n {
			if i == j do continue

			if cast(int)registry.systems[j].info.stage < cast(int)registry.systems[i].info.stage {
				dag.dep_masks[i] |= dag.self_bits[j]
			}
		}
	}

	// Same-stage conflict ordering: preserve deterministic order from registry.
	for i in 0 ..< n {
		for j in 0 ..< i {
			if registry.systems[j].info.stage != registry.systems[i].info.stage do continue

			if task_conflict(&dag.task_ids[j], &dag.task_ids[i]) {
				dag.dep_masks[i] |= dag.self_bits[j]
			}
		}
	}

	// Explicit user dependencies from registry.
	for dep in registry.dependencies {
		before_idx := dag_find_system_index_by_id(registry, dep.before)
		after_idx := dag_find_system_index_by_id(registry, dep.after)

		if before_idx < 0 || after_idx < 0 || before_idx == after_idx do continue

		dag.dep_masks[after_idx] |= dag.self_bits[before_idx]
	}

	// ---------------------------------------------------------
	// 3. Metadata passes (SOA ONLY)
	// ---------------------------------------------------------
	dag_assign_numa(&dag)
	dag_assign_worker_affinity_compile(&dag, worker_count)
	dag_compute_depths(&dag)
	dag_assign_cache_groups(&dag)
	dag_estimate_costs(&dag)
	dag_build_dependents(&dag, allocator)
	dag_compile_owner_worker(&dag, worker_count)

	return dag
}

dag_find_system_index_by_id :: proc(registry: ^System_Registry, id: Core.System_ID) -> int {
	for i in 0 ..< len(registry.systems) {
		if registry.systems[i].id == id do return i
	}
	return -1
}

dag_build_dependents :: proc(dag: ^Frame_DAG, allocator: mem.Allocator) {
	n := len(dag.task_ids)

	// first pass: count dependents
	for i in 0 ..< n {dag.dependents_count[i] = 0}

	for i in 0 ..< n {
		for j in 0 ..< n {
			if i == j do continue

			// if j depends on i → i is dependency of j
			if (dag.dep_masks[j] & dag.self_bits[i]) != 0 {
				dag.dependents_count[i] += 1
			}
		}
	}

	// prefix sum for offsets
	offset := 0
	for i in 0 ..< n {
		dag.dependents_start[i] = i32(offset)
		offset += int(dag.dependents_count[i])
	}

	dag.dependents_flat = make([]i32, offset, allocator)

	// temp counters
	cursor := make([]i32, n, allocator)
	for i in 0 ..< n {
		cursor[i] = dag.dependents_start[i]
	}

	// fill adjacency list
	for i in 0 ..< n {
		for j in 0 ..< n {
			if i == j do continue

			if (dag.dep_masks[j] & dag.self_bits[i]) != 0 {
				idx := cursor[i]
				dag.dependents_flat[idx] = i32(j)
				cursor[i] += 1
			}
		}
	}
	delete(cursor, allocator)
}

dag_compile_owner_worker :: proc(dag: ^Frame_DAG, worker_count: int) {
	for i in 0 ..< len(dag.task_ids) {
		worker := int(dag.preferred_worker[i])

		if worker < 0 || worker >= worker_count {
			worker = i % worker_count
		}

		dag.owner_worker[i] = i16(worker)
	}
}

dag_assign_worker_affinity_compile :: proc(dag: ^Frame_DAG, worker_count: int) {
	for i in 0 ..< len(dag.task_ids) {
		worker := i % worker_count

		if dag.preferred_numa[i] >= 0 {
			worker = (worker + int(dag.preferred_numa[i])) % worker_count
		}

		dag.preferred_worker[i] = i16(worker)
	}
}

dag_compute_depths :: proc(dag: ^Frame_DAG) {
	n := len(dag.task_ids)

	// initialize
	for i in 0 ..< n {dag.depth[i] = 0}

	// propagate depth using bitmasks
	for i in 0 ..< n {
		for j in 0 ..< n {
			if (dag.dep_masks[i] & dag.self_bits[j]) != 0 {
				if dag.depth[j] < dag.depth[i] + 1 {
					dag.depth[j] = dag.depth[i] + 1
				}
			}
		}
	}
}

dag_assign_cache_groups :: proc(dag: ^Frame_DAG) {
	n := len(dag.task_ids)

	for i in 0 ..< n {dag.cache_group[i] = u32(i & 3)}
}

dag_estimate_costs :: proc(dag: ^Frame_DAG) {
	for i in 0 ..< len(dag.task_ids) {
		cost: f32 = 1.0
		cost += f32(dag.depth[i])

		if (dag.dep_masks[i] != 0) {cost += 0.5}

		dag.cost_estimate[i] = cost
	}
}

dag_sort_islands :: proc(islands: []Frame_Island) {
	// Pass islands as a slice parameter instead of accessing through dag
	for i in 1 ..< len(islands) {
		j := i
		for j > 0 && islands[j - 1].pred_cost < islands[j].pred_cost {
			tmp := islands[j]
			islands[j] = islands[j - 1]
			islands[j - 1] = tmp
			j -= 1
		}
	}
}

dag_init :: proc(dag: ^Frame_DAG, n: int, worker_count: int, allocator: mem.Allocator) {
	dag.task_ids = make([]Task, n, allocator)
	dag.dep_masks = make([]u64, n, allocator)
	dag.self_bits = make([]u64, n, allocator)

	dag.preferred_worker = make([]i16, n, allocator)
	dag.preferred_numa = make([]i16, n, allocator)
	dag.cache_group = make([]u32, n, allocator)

	dag.depth = make([]i32, n, allocator)
	dag.cost_estimate = make([]f32, n, allocator)

	dag.dependents_start = make([]i32, n, allocator)
	dag.dependents_count = make([]i32, n, allocator)
	dag.dependents_flat = make([]i32, 0, allocator)
	dag.owner_worker = make([]i16, n, allocator)
}

dag_clear :: proc(dag: ^Frame_DAG, allocator: mem.Allocator) {
	// Use delete for slices, not clear
	delete(dag.task_ids, allocator)
	delete(dag.dep_masks, allocator)
	delete(dag.self_bits, allocator)
	delete(dag.preferred_worker, allocator)
	delete(dag.preferred_numa, allocator)
	delete(dag.cache_group, allocator)
	delete(dag.depth, allocator)
	delete(dag.cost_estimate, allocator)
	delete(dag.dependents_start, allocator)
	delete(dag.dependents_count, allocator)
	delete(dag.dependents_flat, allocator)
	delete(dag.owner_worker, allocator)
}

distance_cost :: proc(dag: ^Frame_DAG, node_index: int, w: ^Worker_Context) -> f32 {
	cost: f32 = 0

	if dag.preferred_numa[node_index] == i16(w.numa_node) {
		cost -= 10
	} else {cost += 10}

	if int(dag.cache_group[node_index]) == w.id {cost -= 5}

	return cost
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
