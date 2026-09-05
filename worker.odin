// Engine/src/Modules/BF_DAG/worker.odin
//
// Worker_Context and the worker-thread main loop. The worker thread
// pulls nodes from its own Work_Deque (LIFO via deque_pop), steals from
// peers when its deque is empty, and blocks on frame_active == 0 when
// there is no work to do.
//
// Ported from DagScheduler/Worker.odin (Ymir engine). The worker loop
// is unchanged; the context.allocator rebind that protects system
// callbacks from being tracked under the wrong allocator is preserved.
package BF_DAG

import "core:fmt"
import "core:mem"
import "core:sync"
import "core:thread"

Worker_Context :: struct {
	id:           int,
	steal_cursor: int,
	runtime:      ^Scheduler_Runtime,
	local_queue:  ^Work_Deque,
	allocator:    mem.Allocator,
}

worker_thread_main :: proc(worker: ^Worker_Context) {
	when SCHED_TRACE_VERBOSE {
		fmt.printf(
			"SCHED worker[%d] thread started: runtime=%p local_queue=%p\n",
			worker.id,
			worker.runtime,
			worker.local_queue,
		)
	}

	// Bind this thread's context.allocator to the engine's allocator
	// so that any allocations performed inside system callbacks (e.g.
	// append on a [dynamic] in render_packet_extract_*) are registered
	// with the same allocator that will later free them. Without this,
	// worker threads use the default heap allocator and the tracking
	// allocator flags a Bad free when the matching delete() runs on the
	// main thread.
	context.allocator = worker.allocator

	worker_execute_loop(worker, false)
}

worker_try_execute_one :: proc(worker: ^Worker_Context) -> bool {
	scheduler_drain_external_ready(worker.runtime, worker.id)
	idx, ok := deque_pop(worker.local_queue)
	if !ok {idx = worker_steal_node(worker)}
	if idx < 0 do return false
	return execute_node(idx, worker.runtime, worker.id)
}

worker_execute_loop :: proc(worker: ^Worker_Context, main_thread: bool) {
	runtime := worker.runtime
	budget_warned := false

	for {
		if !runtime.running do return

		if main_thread && sync.atomic_load(&runtime.remaining_tasks) == 0 do return

		if !main_thread && sync.atomic_load(&runtime.frame_active) == 0 {
			budget_warned = false
			thread.yield()
			continue
		}

		frame_budget_update(runtime)

		if runtime.frame_budget.remaining_ms <= 0 {
			when SCHED_TRACE_WARN {
				if !budget_warned && main_thread {
					ready_now := sync.atomic_load(&runtime.ready_tasks)
					remaining_now := sync.atomic_load(&runtime.remaining_tasks)
					if ready_now > 0 &&
					   remaining_now > 0 &&
					   runtime.frame_budget.remaining_ms < -1.0 {
						fmt.printf(
							"SCHED WARN worker[%d] frame budget exhausted: remaining=%.3f ready=%d remaining_tasks=%d\n",
							worker.id,
							runtime.frame_budget.remaining_ms,
							ready_now,
							remaining_now,
						)
					}
				}
			}
			budget_warned = true
		} else {
			budget_warned = false
		}

		if worker_try_execute_one(worker) do continue

		if main_thread {
			remaining_now := sync.atomic_load(&runtime.remaining_tasks)
			ready_now := sync.atomic_load(&runtime.ready_tasks)

			if remaining_now > 0 && ready_now == 0 {
				// No currently ready work exists while work is still
				// outstanding.
				//
				// Do not manufacture completion. Leave the frame alive
				// and let scheduler_wait_frame / diagnostics handle it.
				// Removed stall spin here.
				return
			}
		}
		thread.yield()
	}
}

// ================================================================
// Local Deque Pop (LIFO)
// ================================================================
worker_pop_node :: proc(worker: ^Worker_Context) -> int {
	runtime := worker.runtime

	node, ok := deque_pop(worker.local_queue)

	if !ok {
		when SCHED_TRACE_VERBOSE {
			fmt.printf(
				"SCHED worker[%d] pop failed with ready=%d local(top=%d,bottom=%d)\n",
				worker.id,
				sync.atomic_load(&runtime.ready_tasks),
				sync.atomic_load(&worker.local_queue.top),
				sync.atomic_load(&worker.local_queue.bottom),
			)
		}
		return -1
	}

	when SCHED_TRACE_VERBOSE {
		fmt.printf(
			"SCHED worker[%d] pop node=%d ready=%d remaining=%d\n",
			worker.id,
			node,
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}

	return node
}

//* Work Stealing (pre-computed candidate lists)
worker_steal_node :: proc(worker: ^Worker_Context) -> int {
	count := worker.runtime.worker_count
	if count <= 1 do return -1
	start := worker.steal_cursor % count
	for offset in 1 ..< count {
		victim := (start + offset) % count
		if victim == worker.id do continue
		node, ok := deque_steal(&worker.runtime.deques[victim])
		if ok {
			worker.steal_cursor = (victim + 1) % count
			return node
		}
	}
	worker.steal_cursor = (start + 1) % count
	return -1
}

worker_load_guard :: proc(worker: ^Worker_Context) -> bool {
	runtime := worker.runtime

	// if system is overloaded, slow stealing aggressiveness
	threshold := i32(runtime.worker_count * 64)

	if sync.atomic_load(&runtime.ready_tasks) > threshold {
		return true
	}

	return false
}
