// Engine/src/Modules/BF_DAG/service.odin
//
// Scheduler_Service implementation — the vtable BF_DAG exports to the
// engine. The struct itself lives in Core (Core/SDK-level ABI); BF_DAG
// populates it during module_register() and registers the resulting
// pointer with the global service registry under SCHEDULER_SERVICE_NAME.
//
// Lifetime:
//   * service_create is invoked by the BF_DAG module's register()
//     callback. It allocates a Scheduler_Runtime and the vtable
//     (instance points at the runtime).
//   * service_build is called once by the engine AFTER every module has
//     been activated. The engine walks every module's
//     registration.systems, builds a System_Entry slice, and hands it
//     to the service via rawptr.
//   * service_begin_frame / run / wait form the per-frame driver.
//   * service_destroy tears the runtime down (called automatically by
//     the service registry when the module unloads).
package BF_DAG

import "../../Core"
import "core:log"
import "core:mem"

// ============================================================================
// SERVICE NAME
// ============================================================================
//
// SCHEDULER_SERVICE_NAME is re-exported as Core.BF_DAG_SCHEDULER_SERVICE_NAME
// so consumers do not need a package dependency on BF_DAG. The local
// alias stays for clarity inside this module.
SCHEDULER_SERVICE_NAME :: Core.BF_DAG_SCHEDULER_SERVICE_NAME

// ============================================================================
// SERVICE IMPLS
// ============================================================================
//
// Each impl casts the rawptr arguments back to the Core-defined slice /
// pointer types and forwards to the scheduler_* procs in public.odin.

service_build :: proc(
	service: ^Core.Scheduler_Service,
	systems_ptr: rawptr,
	systems_count: int,
	deps_ptr: rawptr,
	deps_count: int,
	allocator: mem.Allocator,
) -> bool {
	if service == nil || service.instance == nil {
		log.error("[DAG] service_build: null service or instance")
		return false
	}

	runtime := cast(^Scheduler_Runtime)service.instance

	systems: []Core.System_Entry = nil
	if systems_ptr != nil && systems_count > 0 {
		systems = (cast([^]Core.System_Entry)systems_ptr)[:systems_count]
	}
	deps: []Core.System_Dependency = nil
	if deps_ptr != nil && deps_count > 0 {
		deps = (cast([^]Core.System_Dependency)deps_ptr)[:deps_count]
	}

	// Build an internal System_Registry from the caller's slice and
	// hand it to the existing compile pipeline. The registry's backing
	// arrays are owned by the caller; we don't free them.
	registry := System_Registry {
		systems      = systems,
		dependencies = make([dynamic]Core.System_Dependency, len(deps), allocator),
	}
	for dep in deps {
		append(&registry.dependencies, dep)
	}

	scheduler_compile_frame(runtime, &registry)

	delete(registry.dependencies)

	return true
}

service_begin_frame :: proc(service: ^Core.Scheduler_Service, frame_ptr: rawptr) {
	if service == nil || service.instance == nil do return
	if frame_ptr == nil {
		log.error("[DAG] service_begin_frame: null frame")
		return
	}
	runtime := cast(^Scheduler_Runtime)service.instance
	frame := cast(^Core.Scheduler_Frame)frame_ptr
	scheduler_begin_frame(runtime, frame)
}

service_run :: proc(service: ^Core.Scheduler_Service) {
	if service == nil || service.instance == nil do return
	runtime := cast(^Scheduler_Runtime)service.instance
	scheduler_run_main_worker(runtime)
}

service_wait :: proc(service: ^Core.Scheduler_Service) {
	if service == nil || service.instance == nil do return
	runtime := cast(^Scheduler_Runtime)service.instance
	scheduler_wait_frame(runtime)
}

service_start_workers :: proc(service: ^Core.Scheduler_Service) {
	if service == nil || service.instance == nil do return
	runtime := cast(^Scheduler_Runtime)service.instance
	scheduler_start_workers(runtime)
}

service_destroy :: proc(service: ^Core.Scheduler_Service) {
	if service == nil do return
	if service.instance != nil {
		runtime := cast(^Scheduler_Runtime)service.instance
		scheduler_destroy(runtime)
		free(runtime, context.allocator)
		service.instance = nil
	}
}

// ============================================================================
// EXTERNAL NODE + PRE-FRAME HOOK VTABLE IMPLS
// ============================================================================
//
// These are the ABI hooks that modules (BF_GPU, BF_Net, BF_Audio,
// asset streamer, ...) call to wire external synchronization
// boundaries into the DAG without taking a direct package dependency
// on BF_DAG. See Core/scheduler.odin::Scheduler_Service for the
// full ABI surface.

service_external_create :: proc(service: ^Core.Scheduler_Service) -> Core.External_Node_Handle {
	if service == nil || service.instance == nil {
		return Core.EXTERNAL_NODE_HANDLE_INVALID
	}
	runtime := cast(^Scheduler_Runtime)service.instance
	return scheduler_external_create(runtime)
}

service_external_destroy :: proc(service: ^Core.Scheduler_Service, handle: Core.External_Node_Handle) -> bool {
	if service == nil || service.instance == nil do return false
	runtime := cast(^Scheduler_Runtime)service.instance
	scheduler_external_destroy(runtime, handle)
	return true
}

service_external_signal :: proc(service: ^Core.Scheduler_Service, handle: Core.External_Node_Handle) -> bool {
	if service == nil || service.instance == nil do return false
	runtime := cast(^Scheduler_Runtime)service.instance
	return scheduler_external_signal(runtime, handle)
}

service_external_reset :: proc(service: ^Core.Scheduler_Service, handle: Core.External_Node_Handle) -> bool {
	if service == nil || service.instance == nil do return false
	runtime := cast(^Scheduler_Runtime)service.instance
	return scheduler_external_reset(runtime, handle)
}

service_external_wait_for_system_name :: proc(
	service: ^Core.Scheduler_Service,
	handle: Core.External_Node_Handle,
	system_name: cstring,
) -> bool {
	if service == nil || service.instance == nil do return false
	runtime := cast(^Scheduler_Runtime)service.instance
	dag := runtime.active_dag
	if dag == nil do return false

	// compiled_dag.task_ids mirrors the registry.systems ordering used
	// at compile time (see compile_frame_dag in dag.odin), so the
	// node_index is the same as the DAG node_index. Look up by name.
	target := string(system_name)
	node_index := -1
	for task, i in dag.task_ids {
		if task.name == target {
			node_index = i
			break
		}
	}
	if node_index < 0 do return false
	return scheduler_external_wait(runtime, handle, node_index)
}

service_register_pre_frame_hook :: proc(
	service: ^Core.Scheduler_Service,
	hook: Core.Scheduler_Pre_Frame_Hook,
	user_data: rawptr,
) -> bool {
	if service == nil || service.instance == nil do return false
	runtime := cast(^Scheduler_Runtime)service.instance
	return scheduler_register_pre_frame_hook(runtime, hook, user_data)
}

// ============================================================================
// SERVICE FACTORY
// ============================================================================

// new_scheduler_service allocates the vtable and runtime. The runtime
// is created with a worker count based on the host CPU.
//
// Caller is responsible for registering the resulting pointer with
// Core's service registry under SCHEDULER_SERVICE_NAME.
new_scheduler_service :: proc(cpu: CPU_Info, worker_count: int = 0) -> ^Core.Scheduler_Service {
	// Allocate the vtable + runtime together as a single allocation
	// so the service registry owns one block per service.
	block := new(Service_Block, context.allocator)
	block.runtime = new(Scheduler_Runtime, context.allocator)

	effective_worker_count := worker_count
	if effective_worker_count <= 0 {
		if cpu.logical_cores > 1 {
			effective_worker_count = cpu.logical_cores - 1
		} else {
			effective_worker_count = 1
		}
	}

	scheduler_runtime_init(block.runtime, effective_worker_count, context.allocator)

	block.vtable = Core.Scheduler_Service {
		instance                       = rawptr(block.runtime),
		build                          = service_build,
		begin_frame                    = service_begin_frame,
		run                            = service_run,
		wait                           = service_wait,
		start_workers                  = service_start_workers,
		destroy                        = service_destroy,
		worker_count                   = service_worker_count,
		external_create                = service_external_create,
		external_destroy               = service_external_destroy,
		external_signal                = service_external_signal,
		external_reset                 = service_external_reset,
		external_wait_for_system_name  = service_external_wait_for_system_name,
		register_pre_frame_hook        = service_register_pre_frame_hook,
	}

	return &block.vtable
}

// Service_Block pairs the Core.Scheduler_Service vtable with the
// Scheduler_Runtime it points at. The vtable address is what we hand
// to the service registry; the block bookkeeping keeps the runtime
// alive across destroy_scheduler_service.
Service_Block :: struct {
	vtable:  Core.Scheduler_Service,
	runtime: ^Scheduler_Runtime,
}

// destroy_scheduler_service is the Core service destroy proc. Frees
// the runtime (via service.destroy, which also frees the runtime's
// internal buffers) and the Service_Block container.
destroy_scheduler_service :: proc(instance: rawptr) {
	if instance == nil do return
	vtable := cast(^Core.Scheduler_Service)instance
	vtable.destroy(vtable)
	// Recover the Service_Block container. The vtable lives inside
	// the block, so the vtable pointer itself is a tagged pointer back
	// into the block's storage.
	block := cast(^Service_Block)vtable
	free(block, context.allocator)
}
