// Engine/src/Modules/BF_DAG/types.odin
//
// BF_DAG-internal types. The cross-ABI scheduler types (System_Stage,
// System_ID, Access_Mask, System_Info, System_Entry, System_Dependency,
// World_Handle, Engine_Handle, Scheduler_Frame, Scheduler_Service) all
// live in Core/scheduler.odin — the engine and BF_DAG share the layout
// through Core, no package cycle.
//
// This file holds the one type that is private to BF_DAG (System_Registry)
// plus the lightweight CPU_Info used at scheduler construction time.
package BF_DAG

import "../../Core"

// System_Registry is the in-memory registry the DAG compiler walks.
// 'systems' is a borrowed slice (caller owns the storage).
// 'dependencies' is owned by BF_DAG and freed when compile_frame_dag
// returns.
System_Registry :: struct {
	systems:      []Core.System_Entry,
	dependencies: [dynamic]Core.System_Dependency,
}

// CPU_Info describes the host's CPU. Returned by detect_cpu_info() in
// mod.odin and passed into new_scheduler_service() to choose a worker
// count.
CPU_Info :: struct {
	logical_cores:  int,
	physical_cores: int,
}
