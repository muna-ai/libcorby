/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! Additive C ABI. Only opaque handles cross the runtime/client boundary.
use crate::*;
use scheduler::{MemoryProgress, Reclaim, Requirement};
use std::ffi::c_void;

#[repr(C)]
pub struct WorkOptions {
    pub size: u32,
    pub abi_version: u32,
    pub resource: u32,
    pub intent: u32,
}
#[repr(C)]
pub struct AcquireOptions {
    pub size: u32,
    pub abi_version: u32,
    pub flags: u32,
    pub reserved: u32,
    pub timeout_ns: u64,
}
#[repr(C)]
pub struct MemoryOptions {
    pub size: u32,
    pub abi_version: u32,
    pub requirements: *const Requirement,
    pub requirement_count: u32,
    pub reserved: u32,
    pub timeout_ns: u64,
}
#[repr(C)]
pub struct ReleaseReport {
    pub size: u32,
    pub abi_version: u32,
    pub released_bytes: u64,
}
pub struct ReclaimHandle(pub(crate) Arc<Reclaim>);
pub type AcquisitionFn = unsafe extern "C" fn(*mut LeaseHandle, *mut c_void);
pub type MemoryFn = unsafe extern "C" fn(*mut ReclaimHandle, *mut c_void);
pub type ProgressFn = unsafe extern "C" fn(*mut LeaseHandle, *const MemoryProgress, *mut c_void);

pub(crate) fn status(value: i32) -> std::result::Result<Status, Status> {
    Ok(match value {
        0 => Status::Ok,
        1 => Status::InvalidArgument,
        2 => Status::Unsupported,
        3 => Status::Busy,
        4 => Status::Timeout,
        5 => Status::Cancelled,
        6 => Status::Conflict,
        7 => Status::Retired,
        8 => Status::Internal,
        9 => Status::NotFound,
        10 => Status::Unavailable,
        11 => Status::Pending,
        _ => return Err(Status::InvalidArgument),
    })
}

pub unsafe extern "C" fn cor_work_begin_ex(
    engine: *const EngineHandle,
    options: *const WorkOptions,
    result: *mut *mut WorkHandle,
) -> i32 {
    boundary(|| {
        unsafe {
            output(result)?;
        }
        let (resource, intent) = if options.is_null() {
            (0, 0)
        } else {
            unsafe {
                prefix(options, 16)?;
                ((*options).resource, (*options).intent)
            }
        };
        let work = unsafe { reference(engine)? }
            .0
            .begin_ex(resource as usize, intent)?;
        unsafe {
            result.write(Box::into_raw(Box::new(WorkHandle(work))));
        }
        Ok(())
    })
}

pub unsafe extern "C" fn cor_lease_acquire_ex(
    work: *const WorkHandle,
    options: *const AcquireOptions,
    result: *mut *mut LeaseHandle,
) -> i32 {
    boundary(|| {
        unsafe {
            output(result)?;
        }
        let (flags, timeout) = if options.is_null() {
            (0, u64::MAX)
        } else {
            unsafe {
                prefix(options, 24)?;
                if (*options).reserved != 0 {
                    return Err(Status::Unsupported);
                }
                ((*options).flags, (*options).timeout_ns)
            }
        };
        if flags & !1 != 0 {
            return Err(Status::Unsupported);
        }
        let lease = unsafe { reference(work)? }.0.acquire_ex(timeout)?;
        if flags == 0 {
            lease.wait()?;
        }
        unsafe {
            result.write(Box::into_raw(Box::new(LeaseHandle(lease))));
        }
        Ok(())
    })
}

pub unsafe extern "C" fn cor_lease_acquired(lease: *const LeaseHandle) -> i32 {
    boundary(|| unsafe { reference(lease)? }.0.acquired())
}

pub unsafe extern "C" fn cor_lease_register_handler(
    lease: *mut LeaseHandle,
    handler: Option<AcquisitionFn>,
    context: *mut c_void,
) -> i32 {
    boundary(|| {
        let handler = handler.ok_or(Status::InvalidArgument)?;
        let pointer = lease as usize;
        let context = context as usize;
        unsafe { reference(lease)? }
            .0
            .register_handler(Arc::new(move |_| unsafe {
                handler(pointer as *mut LeaseHandle, context as *mut c_void);
            }))
    })
}

pub unsafe extern "C" fn cor_engine_set_memory_handler(
    engine: *const EngineHandle,
    handler: Option<MemoryFn>,
    context: *mut c_void,
) -> i32 {
    boundary(|| {
        let context = context as usize;
        unsafe { reference(engine)? }
            .0
            .set_memory_handler(handler.map(|handler| {
                Arc::new(move |request| {
                    let request = Box::into_raw(Box::new(ReclaimHandle(request)));
                    unsafe {
                        handler(request, context as *mut c_void);
                    }
                }) as Arc<dyn Fn(Arc<Reclaim>) + Send + Sync>
            }))
    })
}

pub unsafe extern "C" fn cor_lease_reclaim_memory(
    lease: *mut LeaseHandle,
    options: *const MemoryOptions,
    handler: Option<ProgressFn>,
    context: *mut c_void,
) -> i32 {
    boundary(|| {
        unsafe {
            prefix(options, std::mem::offset_of!(MemoryOptions, timeout_ns) + 8)?;
        }
        // Read only the frozen v1 prefix, even after this struct grows.
        let (reserved, count, values, timeout) = unsafe {
            (
                ptr::addr_of!((*options).reserved).read_unaligned(),
                ptr::addr_of!((*options).requirement_count).read_unaligned(),
                ptr::addr_of!((*options).requirements).read_unaligned(),
                ptr::addr_of!((*options).timeout_ns).read_unaligned(),
            )
        };
        if reserved != 0 || count == 0 || count > 4096 || values.is_null() {
            return Err(Status::InvalidArgument);
        }
        let requirements = unsafe { std::slice::from_raw_parts(values, count as usize) }.to_vec();
        let handler = handler.ok_or(Status::InvalidArgument)?;
        let pointer = lease as usize;
        let context = context as usize;
        unsafe { reference(lease)? }.0.reclaim_memory(
            requirements,
            timeout,
            Arc::new(move |progress| unsafe {
                handler(
                    pointer as *mut LeaseHandle,
                    &progress,
                    context as *mut c_void,
                );
            }),
        )
    })
}

pub unsafe extern "C" fn cor_reclaim_requirements(
    request: *const ReclaimHandle,
    requirements: *mut Requirement,
    capacity: u32,
    needed: *mut u32,
) -> i32 {
    boundary(|| {
        if needed.is_null() {
            return Err(Status::InvalidArgument);
        }
        let values = unsafe { reference(request)? }.0.requirements();
        unsafe {
            needed.write(values.len() as u32);
        }
        if requirements.is_null() {
            return if capacity == 0 {
                Ok(())
            } else {
                Err(Status::InvalidArgument)
            };
        }
        if (capacity as usize) < values.len() {
            return Err(Status::Busy);
        }
        unsafe {
            ptr::copy_nonoverlapping(values.as_ptr(), requirements, values.len());
        }
        Ok(())
    })
}

pub unsafe extern "C" fn cor_reclaim_complete(
    request: *mut ReclaimHandle,
    outcome: i32,
    report: *const ReleaseReport,
) -> i32 {
    boundary(|| {
        let bytes = if report.is_null() {
            0
        } else {
            unsafe {
                prefix(report, 16)?;
                (*report).released_bytes
            }
        };
        unsafe { reference(request)? }
            .0
            .complete(status(outcome)?, bytes)?;
        unsafe {
            drop(Box::from_raw(request));
        }
        Ok(())
    })
}
