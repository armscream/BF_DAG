// Engine/src/Modules/BF_DAG/public.odin
//
// Public-facing scheduler API. The engine drives a frame through these
// procs in this order:
//
//   scheduler_runtime_init     — allocate workers / deques / threads
//   scheduler_compile_frame    — build the DAG from a System_Registry
//   scheduler_start_workers    — spawn the worker thread pool
//   scheduler_begin_frame      — reset per-frame state and enqueue roots
//   scheduler_run_main_worker  — drain the DAG on the calling thread
//   scheduler_wait_frame       — block until all workers finish
//   ... repeat begin/run/wait per frame ...
//   scheduler_destroy          — tear down workers and the DAG
//
// Ported from DagScheduler/Public.odin (Ymir engine). The lifecycle
// sequence is unchanged; only the registry type and the engine-side
// ownership story are different.
package BF_DAG

import "../../Core"
import "core:fmt"
import "core:mem"
import "core:sync"
import "core:thread"

// compile DAG from system registry
scheduler_compile_frame :: proc(runtime: ^Scheduler_Runtime, registry: ^System_Registry) {
	if len(runtime.node_runtime) > 0 {
		delete(runtime.node_runtime)
		runtime.node_runtime = nil
	}
	if len(runtime.compiled_dag.task_ids) > 0 {
		dag_clear(&runtime.compiled_dag, runtime.allocator)
	}

	runtime.compiled_dag = compile_frame_dag(registry, runtime.worker_count, runtime.allocator)
	runtime.active_dag = &runtime.compiled_dag
	runtime.node_runtime = make(
		[dynamic]Node_Runtime,
		len(runtime.compiled_dag.task_ids),
		runtime.allocator,
	)

	for i in 0 ..< len(runtime.compiled_dag.task_ids) {
		runtime.node_runtime[i] = Node_Runtime {
			remaining = runtime.compiled_dag.dependencies_count[i],
			state     = NODE_WAITING,
		}
	}

	runtime.remaining_tasks = 0
	runtime.ready_tasks = 0

	when SCHED_TRACE_SUMMARY {
		fmt.printf(
			"BF_DAG compiled: systems=%d edges=%d\n",
			len(runtime.compiled_dag.task_ids),
			len(runtime.compiled_dag.dependencies_flat),
		)
	}
}

scheduler_runtime_init :: proc(
	runtime: ^Scheduler_Runtime,
	worker_count: int,
	allocator: mem.Allocator = context.allocator,
) {
	effective_worker_count := worker_count
	when SCHED_FORCE_SINGLE_THREAD {
		effective_worker_count = 1
	}

	runtime.allocator = allocator
	runtime.worker_count = effective_worker_count
	runtime.running = true

	runtime.workers = make([]Worker_Context, runtime.worker_count, allocator)
	runtime.deques = make([]Work_Deque, runtime.worker_count, allocator)
	runtime.threads = make([]^thread.Thread, runtime.worker_count, allocator)

	runtime.next_worker = 0
	runtime.ready_tasks = 0
	runtime.remaining_tasks = 0
	runtime.inflight_exec = 0
	runtime.current_pass = 0
	runtime.workers_started = false
	runtime.frame_active = 0
	runtime.frame_gen = 0

	// IMPORTANT: ensure clean state before threads start
	runtime.active_dag = nil

	// --------------------------------------------------------
	// Allocate worker + deque buffers (fully initialized first)
	// --------------------------------------------------------
	for i in 0 ..< runtime.worker_count {
		runtime.deques[i].buffer = make([]int, DEQUE_CAPACITY, allocator)
		sync.atomic_store(&runtime.deques[i].top, 0)
		sync.atomic_store(&runtime.deques[i].bottom, 0)

		runtime.workers[i] = Worker_Context {
			id          = i,
			runtime     = nil,
			local_queue = nil,
			allocator   = allocator,
		}
	}

	// --------------------------------------------------------
	// DO NOT START THREADS YET
	// --------------------------------------------------------
	// (important: prevents race with active_dag setup)
}

scheduler_rebind_worker_contexts :: proc(
	runtime: ^Scheduler_Runtime,
	allocator: mem.Allocator = context.allocator,
) {
	for i in 0 ..< runtime.worker_count {
		runtime.workers[i].id = i
		runtime.workers[i].runtime = runtime
		runtime.workers[i].local_queue = &runtime.deques[i]
		runtime.workers[i].allocator = allocator

		when SCHED_TRACE_VERBOSE {
			fmt.printf(
				"SCHED bind worker[%d]: runtime=%p local_queue=%p\n",
				i,
				runtime,
				runtime.workers[i].local_queue,
			)
		}
	}
}

scheduler_start_workers :: proc(runtime: ^Scheduler_Runtime) {
	if runtime.workers_started do return

	scheduler_rebind_worker_contexts(runtime)

	when SCHED_FORCE_SINGLE_THREAD {
		runtime.workers_started = true
		return
	}

	for i in 1 ..< runtime.worker_count {
		runtime.threads[i] = thread.create_and_start_with_poly_data(
			&runtime.workers[i],
			worker_thread_main,
		)
	}

	runtime.workers_started = true
}

scheduler_destroy :: proc(runtime: ^Scheduler_Runtime) {
	runtime.running = false

	sync.mutex_lock(&runtime.wake_mutex)
	sync.cond_broadcast(&runtime.wake_cond)
	sync.mutex_unlock(&runtime.wake_mutex)

	sync.mutex_lock(&runtime.frame_mutex)
	sync.cond_broadcast(&runtime.frame_cond)
	sync.mutex_unlock(&runtime.frame_mutex)

	if runtime.workers_started {
		for i in 1 ..< runtime.worker_count {
			if runtime.threads[i] != nil {
				if !thread.is_done(runtime.threads[i]) {
					thread.terminate(runtime.threads[i], 0)
				}
				thread.join(runtime.threads[i])
				// Frees the ^Thread itself (allocated via
				// context.allocator at create time). Skipping
				// this is the source of the N x 272-byte
				// thread_windows.odin leaks at shutdown.
				thread.destroy(runtime.threads[i])
				runtime.threads[i] = nil
			}
		}

		runtime.workers_started = false
	}

	alloc := runtime.allocator

	for i in 0 ..< len(runtime.deques) {
		delete(runtime.deques[i].buffer, alloc)
	}

	// node_runtime was created with runtime.allocator; its dynamic
	// array header carries that allocator, so plain `delete` is
	// correct.
	delete(runtime.node_runtime)
	dag_clear(&runtime.compiled_dag, alloc)

	delete(runtime.deques, alloc)
	delete(runtime.workers, alloc)
	delete(runtime.threads, alloc)

	runtime.active_dag = nil
}

//* Frame Begin
scheduler_begin_frame :: proc(runtime: ^Scheduler_Runtime, frame: ^Core.Scheduler_Frame) {
	dag := runtime.active_dag
	frame_budget_begin(runtime, 16.6)

	runtime.active_frame = frame
	runtime.remaining_tasks = i32(len(dag.task_ids))
	runtime.ready_tasks = 0

	when SCHED_TRACE_SUMMARY {
		fmt.printf(
			"SCHED begin frame: tasks=%d workers=%d\n",
			len(dag.task_ids),
			runtime.worker_count,
		)
	}
	for i in 0 ..< len(runtime.deques) {deque_reset(&runtime.deques[i])}
	ready_count: i32 = 0
	// Root nodes are initially placed on worker 0.
	// Worker 0 is the engine/main thread and owns this deque.
	// other workers will steal from it.
	for i in 0 ..< len(dag.task_ids) {
		if dag.dependencies_count[i] != 0 {
			continue
		}

		rt := &runtime.node_runtime[i]
		if sync.atomic_compare_exchange_weak(&rt.state, NODE_WAITING, NODE_READY) != NODE_WAITING do continue
		if !deque_push(&runtime.deques[0], i) {panic("BF_DAG: failed to enqueue root node")}
		ready_count += 1
	}
	sync.atomic_store(&runtime.ready_tasks, ready_count)

	for i in 0 ..< len(dag.task_ids) {
		rt := &runtime.node_runtime[i]
		sync.atomic_store(&rt.remaining, dag.dependencies_count[i])
		sync.atomic_store(&rt.state, NODE_WAITING)
	}

	when SCHED_TRACE_SUMMARY {
		fmt.printf(
			"SCHED frame roots enqueued: ready=%d remaining=%d\n",
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}

	when SCHED_TRACE_VERBOSE {
		for i in 0 ..< len(runtime.deques) {
			fmt.printf(
				"SCHED deque[%d]: top=%d bottom=%d\n",
				i,
				sync.atomic_load(&runtime.deques[i].top),
				sync.atomic_load(&runtime.deques[i].bottom),
			)
		}
	}
	sync.atomic_store(&runtime.frame_active, 1)
}

scheduler_run_main_worker :: proc(runtime: ^Scheduler_Runtime) {
	when SCHED_TRACE_VERBOSE {
		fmt.printf(
			"SCHED main worker entering loop: ready=%d remaining=%d\n",
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}

	worker_execute_loop(&runtime.workers[0], true)

	when SCHED_TRACE_VERBOSE {
		fmt.printf(
			"SCHED main worker exited loop: ready=%d remaining=%d\n",
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}
}

scheduler_run_main_worker_slice :: proc(runtime: ^Scheduler_Runtime, max_steps: int = 1) -> int {
	if max_steps <= 0 do return 0

	ran: int = 0
	for _ in 0 ..< max_steps {
		if !worker_try_execute_one(&runtime.workers[0]) {break}
		ran += 1
	}
	return ran
}

// scheduler_wait_frame blocks the calling thread until the current frame
// is drained. Implementation uses a per-frame generation counter that
// workers bump via node_complete; the main thread spins with backoff and
// only falls back to a cond_wait if the generation hasn't advanced in
// a while. Eliminates the per-frame mutex acquire that the old
// cond-var-only path took.
scheduler_wait_frame :: proc(runtime: ^Scheduler_Runtime) {
	when SCHED_TRACE_SUMMARY {
		fmt.printf(
			"SCHED wait start: ready=%d remaining=%d\n",
			sync.atomic_load(&runtime.ready_tasks),
			sync.atomic_load(&runtime.remaining_tasks),
		)
	}

	gen_start := sync.atomic_load(&runtime.frame_gen)

	// Fast path: spin briefly while the frame completes. Most frames
	// drain within microseconds once all workers have woken up.
	spins: int = 0
	for runtime.running {
		gen_now := sync.atomic_load(&runtime.frame_gen)
		if gen_now != gen_start {
			break
		}
		if sync.atomic_load(&runtime.inflight_exec) == 0 &&
		   sync.atomic_load(&runtime.remaining_tasks) == 0 {
			break
		}

		spins += 1
		switch {
		case spins < 64:
			// tight spin
			continue
		case spins < 1024:
			thread.yield()
		case spins < 8192:
			sync.atomic_thread_fence(.Acquire)
			thread.yield()
		case:
			// Slow path: park on the cond var. Workers still wake
			// the cond var in node_complete; this is purely an
			// energy-friendly fallback for the rare idle frame.
			sync.mutex_lock(&runtime.frame_mutex)
			if sync.atomic_load(&runtime.frame_gen) == gen_start {
				sync.cond_wait(&runtime.frame_cond, &runtime.frame_mutex)
			}
			sync.mutex_unlock(&runtime.frame_mutex)
			break
		}
	}

	sync.atomic_store(&runtime.frame_active, 0)

	// Wait for any in-flight execute_node to fully unwind. On Windows
	// and Linux this is a kernel-level wait — no spin loop.
	for sync.atomic_load(&runtime.inflight_exec) > 0 && runtime.running {
		thread.yield()
	}

	when SCHED_TRACE_SUMMARY {fmt.println("SCHED wait complete")}
}

scheduler_create :: proc(
	runtime: ^Scheduler_Runtime,
	cpu: CPU_Info,
	worker_count: int = 0,
	task_capacity: int = 4096,
) -> Scheduler_Runtime {
	effective_worker_count := worker_count

	if effective_worker_count <= 0 {
		if cpu.logical_cores > 1 {
			effective_worker_count = cpu.logical_cores - 1
		} else {
			effective_worker_count = 1
		}
	}

	scheduler := Scheduler_Runtime{}
	scheduler_runtime_init(&scheduler, effective_worker_count)

	return scheduler
}

// Exposed API, mostly for renderer gpu fences, networking, async asset streaming, audio callbacks, OS events, procedural generation,
// & editor background jobs. Without introducing fibers.
scheduler_external_create :: proc(
	runtime: ^Scheduler_Runtime,
	name: string,
) -> External_Node_Handle
scheduler_external_signal :: proc(runtime: ^Scheduler_Runtime, handle: External_Node_Handle)
