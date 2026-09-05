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

SCHEDULER_SERVICE_NAME :: "BF_DAG.Scheduler"

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
		instance      = rawptr(block.runtime),
		build         = service_build,
		begin_frame   = service_begin_frame,
		run           = service_run,
		wait          = service_wait,
		start_workers = service_start_workers,
		destroy       = service_destroy,
		worker_count  = service_worker_count,
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
