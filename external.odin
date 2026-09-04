// BF_DAG/external.odin
package BF_DAG

External_Node_Handle :: struct {
	index:      u32,
	generation: u32,
}
External_Node_State :: enum u8 {
	Free,
	Armed,
	Signaled,
}
External_Node :: struct {
	generation: u32,
	state:      External_Node_State,
	// Nodes in the compiled DAG that are waiting for this external completion.
	waiters:    [dynamic]int,
}
// Exposed API, mostly for renderer gpu fences, networking, async asset streaming, audio callbacks, OS events, procedural generation,
// & editor background jobs. Without introducing fibers.
scheduler_external_create :: proc(
	runtime: ^Scheduler_Runtime,
	name: string,
) -> External_Node_Handle
scheduler_external_signal :: proc(runtime: ^Scheduler_Runtime, handle: External_Node_Handle)
scheduler_external_wait :: proc(runtime: ^Scheduler_Runtime, handle: External_Node_Handle, node_index: int)