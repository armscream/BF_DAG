// Engine/src/Modules/BF_DAG/scheduler.odin
//
// Scheduler_Runtime and the per-node runtime state that drives the
// ready/claimed/completed state machine. Most of the heavy logic
// (deque pop, steal, execute, wake) also lives here because the
// transitions cross the runtime / worker boundary; splitting it
// across files would just create an artificial seam.
//
// Ported from DagScheduler/Scheduler.odin (Ymir engine). The runtime
// data layout is unchanged; only the system callback signature moved
// from (world, engine, dt) to (rawptr -> Scheduler_Frame).
package BF_DAG

import "../../Core"
import "core:fmt"
import "core:mem"
import "core:sync"
import "core:thread"

SCHED_TRACE_SUMMARY :: false
SCHED_TRACE_WARN :: true
SCHED_TRACE_VERBOSE :: false
SCHED_TRACE :: SCHED_TRACE_VERBOSE

// Temporary safety mode while lock-free multithread scheduler is being
// stabilized.
SCHED_FORCE_SINGLE_THREAD :: false
SCHED_ENABLE_STEALING :: true

Node_Runtime :: struct {
	remaining: i32,
	state:     i32,
}

Node_Kind :: enum u8{
	System,
	External,
}

NODE_WAITING :: i32(0)
NODE_READY :: i32(1)
NODE_RUNNING :: i32(2)
NODE_COMPLETE :: i32(3)

Scheduler_Runtime :: struct {
	allocator:       mem.Allocator,
	worker_count:    int,
	workers:         []Worker_Context,
	threads:         []^thread.Thread,
	node_runtime:    [dynamic]Node_Runtime,
	deques:          []Work_Deque,
	active_dag:      ^Frame_DAG,
	compiled_dag:    Frame_DAG,
	active_frame:    ^Core.Scheduler_Frame,
	global_tick:     u64,
	current_pass:    i32,
	next_worker:     int,
	running:         bool,
	workers_started: bool,
	frame_active:    i32,
	// frame_gen is bumped exactly once per drained frame, by the worker
	// that observed remaining_tasks reach 0. The main thread reads it
	// in scheduler_wait_frame instead of taking a mutex on every frame.
	frame_gen:       u64,
	inflight_exec:   i32,
	ready_tasks:     i32,
	remaining_tasks: i32,
	frame_mutex:     sync.Mutex,
	frame_cond:      sync.Cond,
	wake_mutex:      sync.Mutex,
	wake_cond:       sync.Cond,
	frame_budget:    Frame_Budget,
}

// NUMA_Partition struct needed for this to work
NUMA_Partition :: struct {
	numa_node: i16,
	nodes:     [dynamic]int, // Store node INDICES, not pointers
}

scheduler_reconcile_ready :: proc(runtime: ^Scheduler_Runtime) -> i32 {
	ready: i32 = 0

	for i in 0 ..< len(runtime.deques) {
		t := sync.atomic_load(&runtime.deques[i].top)
		b := sync.atomic_load(&runtime.deques[i].bottom)
		if b > t {
			ready += b - t
		}
	}

	sync.atomic_store(&runtime.ready_tasks, ready)
	return ready
}

scheduler_ready_inc :: proc(runtime: ^Scheduler_Runtime) {
	sync.atomic_add(&runtime.ready_tasks, 1)
}

scheduler_ready_dec :: proc(runtime: ^Scheduler_Runtime) {
	for {
		old := sync.atomic_load(&runtime.ready_tasks)
		if old <= 0 do return

		prev := sync.atomic_compare_exchange_weak(&runtime.ready_tasks, old, old - 1)
		if prev == old do return
	}
}

// ================================================================
// Inject Task (from frame graph / system builder)
// ================================================================
scheduler_submit_node :: proc(runtime: ^Scheduler_Runtime, node_index: int, worker_id: int) {
	rt := &runtime.node_runtime[node_index]

	if sync.atomic_compare_exchange_weak(&rt.executed, NODE_WAITING, NODE_QUEUED) != NODE_WAITING do return

	when SCHED_TRACE_VERBOSE {fmt.printf(
			"SCHED submit node %d to worker %d\n",
			node_index,
			worker_id,
		)}

	// force ready by clearing deps
	rt.dependency_mask = 0

	w := worker_id
	if w < 0 || w >= runtime.worker_count {w = 0}

	if deque_push(&runtime.deques[w], node_index) {
		scheduler_ready_inc(runtime)
	} else {
		sync.atomic_store(&rt.executed, NODE_WAITING)
		when SCHED_TRACE_WARN {
			fmt.printf("SCHED WARN submit drop node=%d worker=%d queue_full\n", node_index, w)
		}
	}
}

// ================================================================
// Steal Interface (used by workers)
// ================================================================
scheduler_steal :: proc(runtime: ^Scheduler_Runtime, thief_id: int) -> int {
	for i in 0 ..< runtime.worker_count {
		if i == thief_id do continue
		idx, ok := deque_steal(&runtime.deques[i])
		if ok do return idx
	}
	return -1
}

// ================================================================
// Worker Access Helpers
// ================================================================

scheduler_get_worker :: proc(runtime: ^Scheduler_Runtime, id: int) -> ^Worker_Context {
	if id < 0 do return nil
	if id >= runtime.worker_count do return nil
	return &runtime.workers[id]
}

wake_dependents :: proc(node_index: int, runtime: ^Scheduler_Runtime, producer_worker: int) {
	dag := runtime.active_dag
	start := dag.dependents_start[node_index]
	count := dag.dependents_count[node_index]

	when SCHED_TRACE_VERBOSE {fmt.printf("SCHED wake dependents of node %d\n", node_index)}

	for k in 0 ..< count {
		dependent := int(dag.dependents_flat[start + k])
		rt := &runtime.node_runtime[dependent]
		previous := sync.atomic_sub(&rt.remaining, 1)
		if previous != 1 do continue
		// This dependency was the final one
		if sync.atomic_compare_exchange_weak(&rt.state, NODE_WAITING, NODE_READY) != NODE_WAITING do continue 
		scheduler_enqueue_ready(runtime, dependent, producer_worker)
	}
}

scheduler_enqueue_ready :: proc(runtime: ^Scheduler_Runtime, node_index: int, producer_worker: int){
	dag := runtime.active_dag
	preferred := int(dag.preferred_worker[node_index])
	target := producer_worker
	if preferred >0 && preferred < runtime.worker_count {
		// TODO: For now, producer locality wins, preferred worker remains only a hint.
	}
	if target < 0 || target >= runtime.worker_count {target = 0}
	if target == producer_worker {
		if deque_push(&runtime.deques[target], node_index) {
			sync.atomic_add(&runtime.ready_tasks, 1)
			return
		}
	}
	worker_inbox_push(&runtime.workers[target], node_index)
	sync.atomic_add(&runtime.ready_tasks, 1)
}

execute_node :: proc(node_index: int, runtime: ^Scheduler_Runtime, worker_id: int) -> bool {
	dag := runtime.active_dag
	rt := &runtime.node_runtime[node_index]

	claim_prev := sync.atomic_compare_exchange_weak(&rt.executed, NODE_QUEUED, NODE_CLAIMED)
	if claim_prev != NODE_QUEUED {
		when SCHED_TRACE_VERBOSE {
			if claim_prev == NODE_CLAIMED {
				fmt.printf(
					"SCHED duplicate execute suppressed for node=%d state=%d\n",
					node_index,
					claim_prev,
				)
			}

			if claim_prev == NODE_WAITING {
				fmt.printf(
					"SCHED stale execute suppressed for node=%d state=%d\n",
					node_index,
					claim_prev,
				)
			}
		}
		return false
	}

	// Decrement ready count at claim time to avoid cross-frame race with wait/begin reset.
	scheduler_ready_dec(runtime)

	task := &dag.task_ids[node_index]

	when SCHED_TRACE_VERBOSE {
		fmt.printf("SCHED executing node %d (%s)\n", node_index, task.name)
		fmt.printf(
			"SCHED execute start node=%d ready=%d remaining=%d\n",
			node_index,
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}

	// Adapter: the new System_Registration ABI is proc(rawptr). Pass
	// the Scheduler_Frame pointer; systems cast back to ^Scheduler_Frame.
	if task.fn != nil {task.fn(rawptr(runtime.active_frame))}

	wake_dependents(node_index, runtime, worker_id)
	node_complete(node_index, runtime)

	when SCHED_TRACE_VERBOSE {
		fmt.printf(
			"SCHED execute end node=%d ready=%d remaining=%d\n",
			node_index,
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}

	return true
}

node_complete :: proc(node_index: int, runtime: ^Scheduler_Runtime) {
	remaining_before := sync.atomic_sub(&runtime.remaining_tasks, 1)
	remaining_after := remaining_before - 1

	when SCHED_TRACE_VERBOSE {fmt.printf(
			"SCHED node %d completed. Remaining tasks: %d\n",
			node_index,
			remaining_after,
		)}

	if remaining_after == 0 {
		sync.atomic_store(&runtime.frame_active, 0)
		// Bump the frame generation so scheduler_wait_frame can stop
		// spinning without taking the mutex on the fast path.
		sync.atomic_add(&runtime.frame_gen, 1)
		// Also broadcast the cond var so any thread that fell through
		// to the slow-path park can wake up.
		sync.mutex_lock(&runtime.frame_mutex)
		sync.cond_broadcast(&runtime.frame_cond)
		sync.mutex_unlock(&runtime.frame_mutex)
	}
}
