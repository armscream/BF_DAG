// Engine/src/Modules/BF_DAG/task.odin
//
// A compiled-DAG node. `fn` is the runtime callback the worker thread
// invokes once the node is claimed; the callback receives a rawptr that
// points to a Scheduler_Frame allocated for the current frame (see
// execute_node in scheduler.odin).
//
// Ported from DagScheduler/Task.odin (Ymir engine). The Ymir version
// passed (world, engine, dt) directly; the Bifrost version unifies on
// the System_Registration.execute ABI (proc(rawptr)) and lets the
// system cast the pointer to ^Scheduler_Frame.
package BF_DAG

import "../../Core"

Task_Proc :: proc(ctx: rawptr)

Task :: struct {
	id:              Core.System_ID,
	name:            string,
	fn:              Task_Proc,
	read_mask:       Core.Access_Mask,
	write_mask:      Core.Access_Mask,

	// compile-time metadata
	simd_compatible: bool,
	state:           Task_State,
}

Task_State :: enum u8 {
	Waiting,
	Ready,
	Running,
	Complete,
}

task_conflict :: proc(a, b: ^Task) -> bool {
	if a == nil || b == nil do return false
	if Core.access_mask_intersects(a.write_mask, b.write_mask) do return true
	if Core.access_mask_intersects(a.write_mask, b.read_mask,) do return true
	if Core.access_mask_intersects(a.read_mask,b.write_mask,) do return true
	return false
}
