// BF_DAG/external.odin
package BF_DAG

import hm "core:container/handle_map"
import "core:container/queue"
import "core:sync"

External_Node_Handle :: hm.Handle32

External_Node :: struct {
	handle:   External_Node_Handle,
	signaled: bool,
	waiters:  [dynamic]int,
}

External_Node_Map :: hm.Dynamic_Handle_Map(External_Node, External_Node_Handle)

// Exposed API, mostly for renderer gpu fences, networking, async asset streaming, audio callbacks, OS events, procedural generation,
// & editor background jobs. Without introducing fibers.
scheduler_external_signal :: proc(
	runtime: ^Scheduler_Runtime,
	handle: External_Node_Handle,
) -> bool {
	sync.mutex_lock(&runtime.external_mutex)

	node, ok := hm.get(&runtime.external_nodes, handle)
	if !ok || node == nil {
		sync.mutex_unlock(&runtime.external_mutex)
		return false
	}

	if node.signaled {
		sync.mutex_unlock(&runtime.external_mutex)
		return true
	}
	node.signaled = true
	// Take ownership of the waiter list while holding
	// the external-node mutex is held.
	waiters := node.waiters[:]
	node.waiters = nil
    frame_active := sync.atomic_load(&runtime.frame_active) != 0
	sync.mutex_unlock(&runtime.external_mutex)
    // if the signal arrived before the frame began, the waiters were never armed into
    // node_runtime.remaining. The frame will therefore simply see this 
    // external node as already signaled.
    if !frame_active {
        delete(waiters)
        return true
    }
	// The external signal arrived during an active frame.
	//
	// We are on an arbitrary thread, so never push directly onto a
	// Chase-Lev worker deque.
	for node_index in waiters {
        if node_index <0 || node_index >= len(runtime.node_runtime) do continue
		rt := &runtime.node_runtime[node_index]
		previous := sync.atomic_sub(&rt.remaining, 1)
		if previous != 1 do continue
		if sync.atomic_compare_exchange_weak(&rt.state, NODE_WAITING, NODE_READY) != NODE_WAITING do continue
		// We cannot safely push onto a worker-owned
		// Chase-Lev deque from this arbitrary thread.
		//
		// This will be handled by the external-ready
		// injection path.
		external_enqueue_ready(runtime, node_index)
	}

	delete(waiters)
	return true
}
scheduler_external_wait :: proc(
	runtime: ^Scheduler_Runtime,
	handle: External_Node_Handle,
	node_index: int,
) -> bool {
	if node_index < 0 || node_index >= len(runtime.node_runtime) do return false

	sync.mutex_lock(&runtime.external_mutex)
	defer sync.mutex_unlock(&runtime.external_mutex)

    // External dependencies are part of frame setup. 
    // Once the frame is active, it's dependency count is fixed.
    if sync.atomic_load(&runtime.frame_active) != 0 do return false

	node, ok:= hm.get(&runtime.external_nodes, handle)
	if !ok || node == nil do return false

	rt := &runtime.node_runtime[node_index]

	if sync.atomic_load(&rt.state) != NODE_WAITING do return false
    // Already completed: no dependency needs to be installed.
	if node.signaled do return true
    // Do not allow the same external gate to be attached to the same DAG node more than once.
    for existing in node.waiters {if existing == node_index do return true}
	append(&node.waiters, node_index)

	sync.atomic_add(&rt.remaining, 1)

	return true
}
scheduler_external_create :: proc(runtime: ^Scheduler_Runtime) -> External_Node_Handle {
	sync.mutex_lock(&runtime.external_mutex)
	defer sync.mutex_unlock(&runtime.external_mutex)

	handle, err := hm.add(&runtime.external_nodes, External_Node{})
	if err != nil {panic("BF_DAG: failed to allocate external node handle")}
    node, ok := hm.get(&runtime.external_nodes, handle)
    if !ok || node == nil do panic("BF_DAG: failed to resolve newly created external node")
    node.handle = handle
	return handle
}
scheduler_external_destroy :: proc(runtime: ^Scheduler_Runtime, handle: External_Node_Handle) {
	sync.mutex_lock(&runtime.external_mutex)
	defer sync.mutex_unlock(&runtime.external_mutex)

	node, ok := hm.get(&runtime.external_nodes, handle)
	if !ok || node == nil do return
    if len(node.waiters) != 0 do panic("BF_DAG: destroying external node with active waiters")
	hm.remove(&runtime.external_nodes, handle)
}
external_enqueue_ready :: proc(runtime: ^Scheduler_Runtime, node_index: int) {
	sync.mutex_lock(&runtime.external_ready_mutex)
	defer sync.mutex_unlock(&runtime.external_ready_mutex)
	ok, err := queue.push_back(&runtime.external_ready, node_index)
	if !ok || err != nil {
		panic("BF_DAG: failed to enqueue external ready node")
	}
	sync.mutex_lock(&runtime.frame_mutex)
	sync.cond_broadcast(&runtime.frame_cond)
	sync.mutex_unlock(&runtime.frame_mutex)
}
scheduler_drain_external_ready :: proc(runtime: ^Scheduler_Runtime, worker_id: int) {
	for {
		node_index: int
		ok: bool
		sync.mutex_lock(&runtime.external_ready_mutex)
		node_index, ok = queue.pop_front_safe(&runtime.external_ready)
		sync.mutex_unlock(&runtime.external_ready_mutex)
		if !ok do break
		if node_index < 0 || node_index >= len(runtime.node_runtime) do continue
		rt := &runtime.node_runtime[node_index]
		if sync.atomic_load(&rt.state) != NODE_READY do continue
		if !deque_push(&runtime.deques[worker_id], node_index) {
			panic("BF_DAG: failed to inject external-ready node")
		}
		sync.atomic_add(&runtime.ready_tasks, 1)
	}
}
scheduler_arm_external_waiters :: proc(runtime: ^Scheduler_Runtime){
    it := hm.iterator_make(&runtime.external_nodes)

    for node, _ in hm.iterate(&it) {
        if node.signaled do continue 
        for node_index in node.waiters {
            if node_index < 0 || node_index >= len(runtime.node_runtime) do continue
            rt := &runtime.node_runtime[node_index]
            sync.atomic_add(&rt.remaining, 1)
        }
    }
}