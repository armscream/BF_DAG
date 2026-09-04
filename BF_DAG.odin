// Engine/src/Modules/BF_DAG/mod.odin
//
// Bifrost DAG scheduler module. Registers a Scheduler_Service with Core
// so the engine can drive a frame:
//
//   1. engine.init activates every module (BF_DAG activates first
//      because every other module depends on it).
//   2. engine.init calls scheduler_build(), which finds the
//      "BF_DAG.Scheduler" service in the global service registry,
//      walks every module's registration.systems, builds a
//      System_Entry slice, and hands it to the service. The service
//      compiles the Frame_DAG and starts the worker thread pool.
//   3. The application loop calls service.begin_frame / run / wait
//      each tick.
//   4. engine.destroy calls scheduler_shutdown(), which calls
//      service.destroy via the service registry's unregister path.
//
// The bulk of the scheduler code is in dag.odin, deque.odin,
// public.odin, scheduler.odin, service.odin, task.odin, types.odin,
// and worker.odin.
package BF_DAG

import "core:log"
import "../../Core"

// ============================================================================
// MODULE_IDENTITY (parsed by rbs)
// ============================================================================
// === MODULE_IDENTITY (parsed by rbs) ===
IDENTITY :: Core.Lib_Descriptor {
	api_version      = Core.LIB_API_VERSION,
	name             = "BF_DAG",
	version          = Core.Version{0, 0, 1},
	author           = "armscream",
	description      = "Directed Acyclic Graph task and systems scheduler — lock-free parallel work-stealing runtime.",
	component_kind   = .Module,
	type             = .Scheduler,
	flags            = {.Runtime, .Provides_Service},
	capabilities     = {.Custom},
	dependencies     = {},
	dependency_count = 0,
}
// === END MODULE_IDENTITY ===

MODULE_API := Core.LIB_API {
	descriptor = IDENTITY,
	load       = module_load,
	register   = module_register,
	activate   = module_activate,
	deactivate = module_deactivate,
	unload     = module_unload,
}

when #config(BUILDING_BF_DAG_DLL, false) {
	@(export)
	bifrost_lib_get_api :: proc() -> ^Core.LIB_API {
		return &MODULE_API
	}
}

// ============================================================================
// MODULE LIFECYCLE
// ============================================================================

module_load :: proc(ctx: ^Core.Lib_Context) -> bool {
	_ = ctx
	log.info("[DAG] loaded")
	return true
}

module_register :: proc(ctx: ^Core.Lib_Context) -> bool {
	// Allocate the Scheduler_Service and register it with Core under
	// SCHEDULER_SERVICE_NAME. The engine calls into the vtable during
	// scheduler_build() and every frame thereafter.
	cpu := detect_cpu_info()

	service := new_scheduler_service(cpu)
	if service == nil {
		log.error("[DAG] failed to allocate scheduler service")
		return false
	}

	api_raw := Core.lib_context_query(
		ctx,
		Core.CORE_LIB_INTERFACE_COMPONENT_REGISTRATION,
		Core.COMPONENT_REGISTRATION_API_VERSION,
	)
	if api_raw == nil {
		log.error("[DAG] component_registration interface unavailable")
		destroy_scheduler_service(rawptr(service))
		return false
	}
	api := cast(^Core.Component_Registration_API)api_raw

	sreg := Core.Service_Registration {
		name     = SCHEDULER_SERVICE_NAME,
		instance = cast(rawptr)service,
		destroy  = destroy_scheduler_service,
	}
	if !api.add_service(ctx, sreg) {
		log.error("[DAG] failed to register scheduler service")
		destroy_scheduler_service(rawptr(service))
		return false
	}

	log.infof("[DAG] scheduler service registered (%d workers)", service_worker_count(service))
	return true
}

module_activate :: proc(ctx: ^Core.Lib_Context) -> bool {
	_ = ctx
	log.info("[DAG] activated — DAG compiles after every module is registered.")
	return true
}

module_deactivate :: proc(ctx: ^Core.Lib_Context) {
	_ = ctx
}

module_unload :: proc(ctx: ^Core.Lib_Context) {
	_ = ctx
	log.info("[DAG] unloaded")
}

// ============================================================================
// CPU DETECTION
// ============================================================================

// detect_cpu_info returns a CPU_Info describing the host. The Ymir
// version called into the OS directly via OS-specific syscalls; the
// Bifrost version returns a conservative default until the engine
// exposes a richer hwloc-style query (TODO: read process CPU affinity).
detect_cpu_info :: proc() -> CPU_Info {
	// TODO: replace with an OS-aware query (GetActiveProcessorCount on
	// Windows, sched_getaffinity on Linux). 4 is a sensible minimum
	// that keeps the work-stealing pool meaningful on small boxes
	// without oversubscribing on bigger ones.
	cores := 4
	return CPU_Info {
		logical_cores  = cores,
		physical_cores = cores, // Not distinguished at this layer.
	}
}

service_worker_count :: proc(service: ^Core.Scheduler_Service) -> int {
	if service == nil || service.instance == nil do return 0
	runtime := cast(^Scheduler_Runtime)service.instance
	return runtime.worker_count
}
