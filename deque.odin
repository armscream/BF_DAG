// Engine/src/Modules/BF_DAG/deque.odin
//
// Chase-Lev work-stealing deque (Le et al., "Correct and Efficient
// Work-Stealing for Weak Memory Models", PPoPP 2013). All accesses to
// `top` and `bottom` go through sync.atomic_*; mixing atomic and
// non-atomic access on a shared variable is a data race and undefined
// behaviour in Odin.
//
// `top` and `bottom` are wrapped into [0, 2*DEQUE_CAPACITY) by every
// store. Only the array index is wrapped via `mod CAP`, so they only
// carry ordering information beyond the low CAP bits and never overflow.
//
// Pushing and popping are owned exclusively by the worker thread that
// owns this deque. Stealing (a CAS on `top`) is performed by remote
// threads.
//
// Ported from DagScheduler/Deque.odin (Ymir engine).
package BF_DAG

import "core:sync"

Work_Deque :: struct {
	buffer: []int,
	top:    i32,
	bottom: i32,
}

DEQUE_CAPACITY :: 1024

// `top` and `bottom` are kept in [0, 2*DEQUE_CAPACITY) by wrapping on
// every store. The full state of the queue at any instant is
// (bottom - top) and (top mod CAP, bottom mod CAP); the high bits of
// top/bottom only carry ordering and wrap trivially. Without this, an
// i32 overflow during long sessions would corrupt the deque.
DEQUE_WRAP :: i32(2 * DEQUE_CAPACITY)

deque_wrap :: proc(v: i32) -> i32 {
	if v >= DEQUE_WRAP do return v - DEQUE_WRAP
	if v < 0 do return v + DEQUE_WRAP
	return v
}

deque_index :: proc(i: i32) -> int {
	// Wrap-safe index for a possibly negative i32 into [0, DEQUE_CAPACITY).
	return int(((i % DEQUE_CAPACITY) + DEQUE_CAPACITY) % DEQUE_CAPACITY)
}

deque_reset :: proc(d: ^Work_Deque) {
	sync.atomic_store(&d.top, 0)
	sync.atomic_store(&d.bottom, 0)
}

deque_push :: proc(d: ^Work_Deque, node_index: int) -> bool {
	b := sync.atomic_load(&d.bottom)
	t := sync.atomic_load(&d.top)

	if b - t >= DEQUE_CAPACITY do return false

	d.buffer[deque_index(b)] = node_index

	// Release fence pairs with steal's acquire load so the written node
	// is visible before the updated bottom.
	sync.atomic_store(&d.bottom, deque_wrap(b + 1))

	return true
}

deque_pop :: proc(d: ^Work_Deque) -> (int, bool) {
	b := sync.atomic_load(&d.bottom) - 1

	// Store the tentative new bottom. If the queue turns out to be
	// empty we restore bottom = top+1 below; we never leave bottom
	// smaller than top in steady state.
	sync.atomic_store(&d.bottom, deque_wrap(b))

	t := sync.atomic_load(&d.top)

	if t > b {
		// Empty. Restore bottom so future pushes see a consistent
		// state.
		sync.atomic_store(&d.bottom, deque_wrap(b + 1))
		return -1, false
	}

	node := d.buffer[deque_index(b)]

	if t < b do return node, true // More than one element: uncontested pop.

	// Last element: race with steal. CAS top from t to t+1.
	prev := sync.atomic_compare_exchange_weak(&d.top, t, deque_wrap(t + 1))

	// Whether we won or lost, the queue is now empty. Restore bottom
	// so bottom >= top invariant holds.
	sync.atomic_store(&d.bottom, deque_wrap(t + 1))

	if prev != t do return -1, false

	return node, true
}

deque_steal :: proc(d: ^Work_Deque) -> (int, bool) {
	t := sync.atomic_load(&d.top)

	// Acquire fence pairs with push's release store: the steal must
	// observe the written node and the updated bottom in order.
	sync.atomic_thread_fence(.Acquire)
	b := sync.atomic_load(&d.bottom)

	if t >= b do return -1, false

	node := d.buffer[deque_index(t)]

	prev := sync.atomic_compare_exchange_weak(&d.top, t, deque_wrap(t + 1))
	if prev != t do return -1, false

	return node, true
}

deque_empty :: proc(d: ^Work_Deque) -> bool {
	return sync.atomic_load(&d.top) >= sync.atomic_load(&d.bottom)
}

deque_clear :: proc(d: ^Work_Deque) {
	sync.atomic_store(&d.top, 0)
	sync.atomic_store(&d.bottom, 0)
}
