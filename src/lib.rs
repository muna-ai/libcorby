/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#![allow(clippy::missing_safety_doc)] // The installed C header defines pointer contracts.

mod extensions;
mod frontend;
pub use extensions::*;
mod ipc;
mod runtime;
mod scheduler;
pub use frontend::{
    AcquisitionOptions, Context, Engine, Error, Lease, MemoryRequest, Resource, Result, Tenancy,
    Work, WorkIntent,
};
pub use runtime::{Runtime, RuntimeOptions, RuntimeScope};
pub use scheduler::{MemoryProgress, Requirement as MemoryRequirement};
#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod tests;

use scheduler::{Coordinator, Engine as BackendEngine, Lease as BackendLease, Work as BackendWork};
use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

pub const ABI_VERSION: u32 = 1;
pub const CAP_COOPERATIVE: u64 = 1;
pub const TENANCY_DEDICATED: u32 = 1;
pub const CAP_MEMORY_RECLAIM: u64 = 2;
pub const FEATURES: u64 = 0x3f; // groups, dedicated demand, cancellation, fairness, memory, async

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok = 0,
    InvalidArgument = 1,
    Unsupported = 2,
    Busy = 3,
    Timeout = 4,
    Cancelled = 5,
    Conflict = 6,
    Retired = 7,
    Internal = 8,
    NotFound = 9,
    Unavailable = 10,
    Pending = 11,
}

#[repr(C)]
pub struct ContextOptions {
    pub size: u32,
    pub abi_version: u32,
    pub required_features: u64,
}

#[repr(C)]
pub struct EngineOptions {
    pub size: u32,
    pub abi_version: u32,
    pub label: *const c_char,
    pub device_ids: *const i32,
    pub device_count: u32,
    pub weight: u32,
    pub tenancy: u32,
    pub reserved: u32,
    pub capabilities: u64,
}

// Frozen prefixes end at named original fields, not sizeof(a growing struct).
const ENGINE_OPTIONS_V1_SIZE: usize = std::mem::offset_of!(EngineOptions, capabilities) + 8;
const CONTEXT_FEATURES_END: usize = std::mem::offset_of!(ContextOptions, required_features) + 8;

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct EngineStats {
    pub size: u32,
    pub abi_version: u32,
    pub active_work: u64,
    pub grants: u64,
    pub held_ns: u64,
    pub waited_ns: u64,
    pub waiting: u32,
    pub holding: u32,
    pub weight: u32,
    pub tenancy: u32,
    pub capabilities: u64,
}

pub struct ContextHandle(Arc<Coordinator>);
pub struct EngineHandle(Arc<BackendEngine>);
pub struct WorkHandle(Arc<BackendWork>);
pub struct LeaseHandle(BackendLease);

fn boundary(f: impl FnOnce() -> std::result::Result<(), Status>) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => Status::Ok as i32,
        Ok(Err(status)) => status as i32,
        Err(_) => {
            if let Some(coordinator) = runtime::active_coordinator() {
                coordinator.cancel_all();
            }
            Status::Internal as i32
        }
    }
}

unsafe fn write_stats(
    stats: EngineStats,
    result: *mut EngineStats,
) -> std::result::Result<(), Status> {
    let size = unsafe { prefix(result, 8)? };
    unsafe {
        ptr::copy_nonoverlapping(
            (&stats as *const EngineStats).cast::<u8>(),
            result.cast::<u8>(),
            size.min(std::mem::size_of::<EngineStats>()),
        )
    };
    Ok(())
}

unsafe fn write_label(
    label: &str,
    buffer: *mut c_char,
    capacity: u32,
    needed: *mut u32,
) -> std::result::Result<(), Status> {
    if needed.is_null() {
        return Err(Status::InvalidArgument);
    }
    let length = label.len() + 1;
    unsafe { needed.write(length as u32) };
    if buffer.is_null() {
        return if capacity == 0 {
            Ok(())
        } else {
            Err(Status::InvalidArgument)
        };
    }
    if (capacity as usize) < length {
        return Err(Status::InvalidArgument);
    }
    unsafe {
        ptr::copy_nonoverlapping(label.as_ptr(), buffer.cast::<u8>(), label.len());
        buffer.add(label.len()).write(0);
    }
    Ok(())
}

pub unsafe extern "C" fn cor_context_engine_ids(
    context: *const ContextHandle,
    ids: *mut u64,
    capacity: u32,
    needed: *mut u32,
) -> i32 {
    boundary(|| {
        if needed.is_null() {
            return Err(Status::InvalidArgument);
        }
        let context = unsafe { reference(context)? };
        let state = context.0.lock()?;
        let count = u32::try_from(state.entries.len()).map_err(|_| Status::Internal)?;
        unsafe { needed.write(count) };
        if ids.is_null() {
            return if capacity == 0 {
                Ok(())
            } else {
                Err(Status::InvalidArgument)
            };
        }
        if capacity < count {
            return Err(Status::Busy);
        }
        for (offset, id) in state.entries.keys().enumerate() {
            unsafe { ids.add(offset).write(*id) };
        }
        Ok(())
    })
}

pub unsafe extern "C" fn cor_context_engine_devices(
    context: *const ContextHandle,
    id: u64,
    devices: *mut i32,
    capacity: u32,
    needed: *mut u32,
) -> i32 {
    boundary(|| {
        if needed.is_null() {
            return Err(Status::InvalidArgument);
        }
        let context = unsafe { reference(context)? };
        let state = context.0.lock()?;
        let entry = state.entries.get(&id).ok_or(Status::NotFound)?;
        let count = entry.devices.len() as u32;
        unsafe { needed.write(count) };
        if devices.is_null() {
            return if capacity == 0 {
                Ok(())
            } else {
                Err(Status::InvalidArgument)
            };
        }
        if capacity < count {
            return Err(Status::InvalidArgument);
        }
        unsafe { ptr::copy_nonoverlapping(entry.devices.as_ptr(), devices, count as usize) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_context_engine_stats(
    context: *const ContextHandle,
    id: u64,
    result: *mut EngineStats,
) -> i32 {
    boundary(|| {
        let context = unsafe { reference(context)? };
        let state = context.0.lock()?;
        let entry = state.entries.get(&id).ok_or(Status::NotFound)?;
        unsafe { write_stats(entry.stats(), result) }
    })
}

pub unsafe extern "C" fn cor_context_engine_set_weight(
    context: *const ContextHandle,
    id: u64,
    weight: u32,
) -> i32 {
    boundary(|| {
        if weight == 0 {
            return Err(Status::InvalidArgument);
        }
        let context = unsafe { reference(context)? };
        context
            .0
            .lock()?
            .entries
            .get_mut(&id)
            .ok_or(Status::NotFound)?
            .weight = weight;
        context.0.notify();
        Ok(())
    })
}

pub unsafe extern "C" fn cor_context_engine_label(
    context: *const ContextHandle,
    id: u64,
    buffer: *mut c_char,
    capacity: u32,
    needed: *mut u32,
) -> i32 {
    boundary(|| {
        let context = unsafe { reference(context)? };
        let state = context.0.lock()?;
        let entry = state.entries.get(&id).ok_or(Status::NotFound)?;
        unsafe { write_label(&entry.label, buffer, capacity, needed) }
    })
}

unsafe fn reference<'a, T>(pointer: *const T) -> std::result::Result<&'a T, Status> {
    // SAFETY: validity, alignment and lifetime are caller obligations in the C API.
    unsafe { pointer.as_ref() }.ok_or(Status::InvalidArgument)
}

unsafe fn output<T>(pointer: *mut *mut T) -> std::result::Result<(), Status> {
    if pointer.is_null() {
        return Err(Status::InvalidArgument);
    }
    unsafe { pointer.write(ptr::null_mut()) };
    Ok(())
}

unsafe fn prefix<T>(pointer: *const T, minimum: usize) -> std::result::Result<usize, Status> {
    if pointer.is_null() {
        return Err(Status::InvalidArgument);
    }
    let bytes = pointer.cast::<u32>();
    let size = unsafe { bytes.read_unaligned() } as usize;
    if size < 8 || size < minimum {
        return Err(Status::InvalidArgument);
    }
    if unsafe { bytes.add(1).read_unaligned() } != ABI_VERSION {
        return Err(Status::Unsupported);
    }
    Ok(size)
}

/// ABI major, not a release number. Compatible extensions retain this value.
pub extern "C" fn cor_abi_version() -> u32 {
    ABI_VERSION
}

pub extern "C" fn cor_features() -> u64 {
    FEATURES
}

/// Every successful open joins this library instance's process-wide domain.
pub unsafe extern "C" fn cor_context_create(
    options: *const ContextOptions,
    result: *mut *mut ContextHandle,
) -> i32 {
    boundary(|| {
        unsafe { output(result)? };
        if !options.is_null() {
            let size = unsafe { prefix(options, 8)? };
            // Version 1's initial eight-byte prefix remains sufficient forever.
            if size >= CONTEXT_FEATURES_END {
                let required =
                    unsafe { ptr::addr_of!((*options).required_features).read_unaligned() };
                if required & !FEATURES != 0 {
                    return Err(Status::Unsupported);
                }
            }
        }
        let coordinator = runtime::active_coordinator().ok_or(Status::Unavailable)?;
        unsafe { result.write(Box::into_raw(Box::new(ContextHandle(coordinator)))) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_context_destroy(context: *mut ContextHandle) -> i32 {
    boundary(|| {
        if !context.is_null() {
            unsafe { drop(Box::from_raw(context)) };
        }
        Ok(())
    })
}

pub unsafe extern "C" fn cor_engine_register(
    context: *const ContextHandle,
    options: *const EngineOptions,
    result: *mut *mut EngineHandle,
) -> i32 {
    boundary(|| {
        unsafe { output(result)? };
        let context = unsafe { reference(context)? };
        unsafe { prefix(options, ENGINE_OPTIONS_V1_SIZE)? };
        // Read covered fields individually. A reference to the entire latest
        // struct would become invalid when a future client passes an old prefix.
        let options = unsafe {
            EngineOptions {
                size: ptr::addr_of!((*options).size).read_unaligned(),
                abi_version: ptr::addr_of!((*options).abi_version).read_unaligned(),
                label: ptr::addr_of!((*options).label).read_unaligned(),
                device_ids: ptr::addr_of!((*options).device_ids).read_unaligned(),
                device_count: ptr::addr_of!((*options).device_count).read_unaligned(),
                weight: ptr::addr_of!((*options).weight).read_unaligned(),
                tenancy: ptr::addr_of!((*options).tenancy).read_unaligned(),
                reserved: ptr::addr_of!((*options).reserved).read_unaligned(),
                capabilities: ptr::addr_of!((*options).capabilities).read_unaligned(),
            }
        };
        if options.reserved != 0
            || options.capabilities & !(CAP_COOPERATIVE | CAP_MEMORY_RECLAIM) != 0
        {
            return Err(Status::Unsupported);
        }
        if options.label.is_null()
            || options.device_ids.is_null()
            || options.device_count == 0
            || options.device_count > 4096
        {
            return Err(Status::InvalidArgument);
        }
        let label = unsafe { CStr::from_ptr(options.label) }
            .to_str()
            .map_err(|_| Status::InvalidArgument)?;
        if label.is_empty() || label.len() > 4096 {
            return Err(Status::InvalidArgument);
        }
        let devices = unsafe {
            std::slice::from_raw_parts(options.device_ids, options.device_count as usize)
        };
        let engine = context.0.register(
            label.to_owned(),
            devices.to_vec(),
            options.weight,
            options.tenancy,
            options.capabilities,
        )?;
        unsafe { result.write(Box::into_raw(Box::new(EngineHandle(engine)))) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_engine_retire(engine: *const EngineHandle) -> i32 {
    boundary(|| unsafe { reference(engine)? }.0.retire())
}

pub unsafe extern "C" fn cor_engine_unregister(engine: *mut EngineHandle) -> i32 {
    boundary(|| {
        if engine.is_null() {
            return Ok(());
        }
        unsafe { reference(engine)? }.0.unregister()?;
        unsafe { drop(Box::from_raw(engine)) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_engine_set_weight(engine: *const EngineHandle, weight: u32) -> i32 {
    boundary(|| unsafe { reference(engine)? }.0.set_weight(weight))
}

pub unsafe extern "C" fn cor_engine_stats(
    engine: *const EngineHandle,
    result: *mut EngineStats,
) -> i32 {
    boundary(|| {
        let engine = unsafe { reference(engine)? };
        let state = engine.0.coordinator.lock()?;
        let entry = &state.entries[&engine.0.id];
        unsafe { write_stats(entry.stats(), result) }
    })
}

pub unsafe extern "C" fn cor_engine_label(
    engine: *const EngineHandle,
    buffer: *mut c_char,
    capacity: u32,
    needed: *mut u32,
) -> i32 {
    boundary(|| {
        let engine = unsafe { reference(engine)? };
        let state = engine.0.coordinator.lock()?;
        let label = &state.entries[&engine.0.id].label;
        unsafe { write_label(label, buffer, capacity, needed) }
    })
}

pub unsafe extern "C" fn cor_work_begin(
    engine: *const EngineHandle,
    result: *mut *mut WorkHandle,
) -> i32 {
    boundary(|| {
        unsafe { output(result)? };
        let work = unsafe { reference(engine)? }.0.begin()?;
        unsafe { result.write(Box::into_raw(Box::new(WorkHandle(work)))) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_work_cancel(work: *const WorkHandle) -> i32 {
    boundary(|| unsafe { reference(work)? }.0.cancel())
}

pub unsafe extern "C" fn cor_work_end(work: *mut WorkHandle) -> i32 {
    boundary(|| {
        if work.is_null() {
            return Ok(());
        }
        if !unsafe { reference(work)? }.0.idle()? {
            return Err(Status::Busy);
        }
        unsafe { drop(Box::from_raw(work)) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_lease_acquire(
    work: *const WorkHandle,
    timeout_ns: u64,
    result: *mut *mut LeaseHandle,
) -> i32 {
    boundary(|| {
        unsafe { output(result)? };
        let work = unsafe { reference(work)? }.0.clone();
        let lease = work.acquire(timeout_ns)?;
        unsafe { result.write(Box::into_raw(Box::new(LeaseHandle(lease)))) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_lease_should_yield(
    lease: *const LeaseHandle,
    min_hold_ns: u64,
    result: *mut u32,
) -> i32 {
    boundary(|| {
        if result.is_null() {
            return Err(Status::InvalidArgument);
        }
        unsafe { result.write(1) }; // On an error, callers must stop admission.
        let should_yield = unsafe { reference(lease)? }.0.should_yield(min_hold_ns)?;
        unsafe { result.write(u32::from(should_yield)) };
        Ok(())
    })
}

pub unsafe extern "C" fn cor_lease_release(lease: *mut LeaseHandle) -> i32 {
    boundary(|| {
        if !lease.is_null() {
            unsafe { reference(lease)? }.0.close()?;
            unsafe { drop(Box::from_raw(lease)) };
        }
        Ok(())
    })
}

// Native dispatch: private backend functions are never exported as cor_* symbols.
#[repr(C)]
pub(crate) struct Dispatch {
    pub size: u32,
    pub abi_version: u32,
    pub features: u64,
    pub context_create: unsafe extern "C" fn(*const ContextOptions, *mut *mut ContextHandle) -> i32,
    pub context_destroy: unsafe extern "C" fn(*mut ContextHandle) -> i32,
    pub context_engine_ids:
        unsafe extern "C" fn(*const ContextHandle, *mut u64, u32, *mut u32) -> i32,
    pub context_engine_stats:
        unsafe extern "C" fn(*const ContextHandle, u64, *mut EngineStats) -> i32,
    pub context_engine_devices:
        unsafe extern "C" fn(*const ContextHandle, u64, *mut i32, u32, *mut u32) -> i32,
    pub context_engine_set_weight: unsafe extern "C" fn(*const ContextHandle, u64, u32) -> i32,
    pub context_engine_label:
        unsafe extern "C" fn(*const ContextHandle, u64, *mut c_char, u32, *mut u32) -> i32,
    pub engine_register: unsafe extern "C" fn(
        *const ContextHandle,
        *const EngineOptions,
        *mut *mut EngineHandle,
    ) -> i32,
    pub engine_retire: unsafe extern "C" fn(*const EngineHandle) -> i32,
    pub engine_unregister: unsafe extern "C" fn(*mut EngineHandle) -> i32,
    pub engine_set_weight: unsafe extern "C" fn(*const EngineHandle, u32) -> i32,
    pub engine_stats: unsafe extern "C" fn(*const EngineHandle, *mut EngineStats) -> i32,
    pub engine_label: unsafe extern "C" fn(*const EngineHandle, *mut c_char, u32, *mut u32) -> i32,
    pub work_begin: unsafe extern "C" fn(*const EngineHandle, *mut *mut WorkHandle) -> i32,
    pub work_cancel: unsafe extern "C" fn(*const WorkHandle) -> i32,
    pub work_end: unsafe extern "C" fn(*mut WorkHandle) -> i32,
    pub lease_acquire: unsafe extern "C" fn(*const WorkHandle, u64, *mut *mut LeaseHandle) -> i32,
    pub lease_should_yield: unsafe extern "C" fn(*const LeaseHandle, u64, *mut u32) -> i32,
    pub lease_release: unsafe extern "C" fn(*mut LeaseHandle) -> i32,
    pub work_begin_ex:
        unsafe extern "C" fn(*const EngineHandle, *const WorkOptions, *mut *mut WorkHandle) -> i32,
    pub lease_acquire_ex: unsafe extern "C" fn(
        *const WorkHandle,
        *const AcquireOptions,
        *mut *mut LeaseHandle,
    ) -> i32,
    pub lease_acquired: unsafe extern "C" fn(*const LeaseHandle) -> i32,
    pub lease_register_handler:
        unsafe extern "C" fn(*mut LeaseHandle, Option<AcquisitionFn>, *mut std::ffi::c_void) -> i32,
    pub engine_set_memory_handler:
        unsafe extern "C" fn(*const EngineHandle, Option<MemoryFn>, *mut std::ffi::c_void) -> i32,
    pub lease_reclaim_memory: unsafe extern "C" fn(
        *mut LeaseHandle,
        *const MemoryOptions,
        Option<ProgressFn>,
        *mut std::ffi::c_void,
    ) -> i32,
    pub reclaim_requirements: unsafe extern "C" fn(
        *const ReclaimHandle,
        *mut scheduler::Requirement,
        u32,
        *mut u32,
    ) -> i32,
    pub reclaim_complete:
        unsafe extern "C" fn(*mut ReclaimHandle, i32, *const ReleaseReport) -> i32,
}
pub(crate) static DISPATCH: Dispatch = Dispatch {
    size: std::mem::size_of::<Dispatch>() as u32,
    abi_version: ABI_VERSION,
    features: FEATURES,
    context_create: cor_context_create,
    context_destroy: cor_context_destroy,
    context_engine_ids: cor_context_engine_ids,
    context_engine_stats: cor_context_engine_stats,
    context_engine_devices: cor_context_engine_devices,
    context_engine_set_weight: cor_context_engine_set_weight,
    context_engine_label: cor_context_engine_label,
    engine_register: cor_engine_register,
    engine_retire: cor_engine_retire,
    engine_unregister: cor_engine_unregister,
    engine_set_weight: cor_engine_set_weight,
    engine_stats: cor_engine_stats,
    engine_label: cor_engine_label,
    work_begin: cor_work_begin,
    work_cancel: cor_work_cancel,
    work_end: cor_work_end,
    lease_acquire: cor_lease_acquire,
    lease_should_yield: cor_lease_should_yield,
    lease_release: cor_lease_release,
    work_begin_ex: cor_work_begin_ex,
    lease_acquire_ex: cor_lease_acquire_ex,
    lease_acquired: cor_lease_acquired,
    lease_register_handler: cor_lease_register_handler,
    engine_set_memory_handler: cor_engine_set_memory_handler,
    lease_reclaim_memory: cor_lease_reclaim_memory,
    reclaim_requirements: cor_reclaim_requirements,
    reclaim_complete: cor_reclaim_complete,
};
