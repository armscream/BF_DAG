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
	id:          int,
	numa_node:   i16,
	cache_group: u32,
	runtime:     ^Scheduler_Runtime,
	local_queue: ^Work_Deque,
	allocator:   mem.Allocator,
}

// Worker_Mask :: bit_set[Scheduler_Worker]
Worker_Mask :: distinct u64

ctz64 :: proc(x: u64) -> int {
	for i in 0 ..< 64 {
		if (x & (1 << u64(i))) != 0 {
			return i
		}
	}
	return -1
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
	runtime := worker.runtime

	idx := worker_pop_node(worker)

	when SCHED_ENABLE_STEALING {
		if idx < 0 {
			idx = worker_steal_node(worker)
		}
	}

	if idx < 0 do return false
	if sync.atomic_load(&runtime.frame_active) == 0 do return false

	dag := runtime.active_dag
	if dag == nil do return true
	if idx >= len(dag.task_ids) do return true

	sync.atomic_add(&runtime.inflight_exec, 1)
	execute_node(idx, runtime, worker.id)
	sync.atomic_sub(&runtime.inflight_exec, 1)

	return true
}

worker_execute_loop :: proc(worker: ^Worker_Context, main_thread: bool) {
	runtime := worker.runtime
	budget_warned := false
	stall_spins := 0

	for {
		if !runtime.running do return

		if main_thread && sync.atomic_load(&runtime.remaining_tasks) == 0 do return

		if !main_thread && sync.atomic_load(&runtime.frame_active) == 0 {
			budget_warned = false
			worker_sleep_hint()
			continue
		}

		frame_budget_update(runtime)

		if runtime.frame_budget.remaining_ms <= 0 {
			when SCHED_TRACE_WARN {
				if !budget_warned && main_thread {
					ready_now := sync.atomic_load(&runtime.ready_tasks)
					remaining_now := sync.atomic_load(&runtime.remaining_tasks)
					if ready_now > 0 && remaining_now > 0 && runtime.frame_budget.remaining_ms < -1.0 {
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

		if worker_try_execute_one(worker) {
			stall_spins = 0
			continue
		}

		if main_thread {
			remaining_now := sync.atomic_load(&runtime.remaining_tasks)
			ready_now := sync.atomic_load(&runtime.ready_tasks)

			if remaining_now > 0 && ready_now == 0 {
				// Let scheduler_wait_frame own the block/deadlock
				// handling path.
				return
			}

			if remaining_now > 0 && ready_now <= 0 {
				stall_spins += 1
				if stall_spins > 200000 {
					when SCHED_TRACE_WARN {
						fmt.printf(
							"SCHED WARN worker[%d] stalled with remaining=%d ready=%d; forcing frame drain\n",
							worker.id,
							remaining_now,
							ready_now,
						)
					}
					sync.atomic_store(&runtime.remaining_tasks, 0)
					sync.mutex_lock(&runtime.frame_mutex)
					sync.cond_broadcast(&runtime.frame_cond)
					sync.mutex_unlock(&runtime.frame_mutex)
					return
				}
			} else {
				stall_spins = 0
			}
		}

		worker_sleep_hint()
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

// ================================================================
// Work Stealing (pre-computed candidate lists)
// ================================================================

worker_steal_node :: proc(worker: ^Worker_Context) -> int {
	runtime := worker.runtime
	deque_count := len(runtime.deques)
	if deque_count <= 1 {
		return -1
	}

	self := worker.id % deque_count
	if self < 0 {
		self += deque_count
	}

	for victim_offset in 1 ..< deque_count {
		victim := (self + victim_offset) % deque_count

		node, ok := deque_steal(&runtime.deques[victim])

		if ok {
			dag := runtime.active_dag
			if dag == nil || node < 0 || node >= len(dag.task_ids) {
				when SCHED_TRACE_WARN {
					fmt.printf(
						"SCHED WARN worker[%d] dropped invalid stolen node=%d from victim=%d\n",
						worker.id,
						node,
						victim,
					)
				}
				continue
			}

			when SCHED_TRACE_VERBOSE {
				fmt.printf(
					"SCHED worker[%d] stole node=%d from worker[%d] ready=%d remaining=%d\n",
					worker.id,
					node,
					victim,
					sync.atomic_load(&runtime.ready_tasks),
					sync.atomic_load(&runtime.remaining_tasks),
				)
			}

			return node
		}
	}

	return -1
}

// ================================================================
// Sleep Hint (NOT blocking engine)
// ================================================================
//
// Replaces the legacy empty `for 0..<100 {}` spin. Yields the thread
// back to the OS scheduler so other workers (and the main thread) can
// run while we're between work units.
worker_sleep_hint :: proc() {
	thread_yield()
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
