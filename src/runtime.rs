/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

use crate::{boundary, prefix, Context, Coordinator, Error, Result, Status, DISPATCH};
use std::ffi::{c_char, CStr};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

extern "C" {
    pub(crate) fn getuid() -> u32;
    fn flock(fd: i32, operation: i32) -> i32;
}
static ACTIVE: Mutex<Option<Arc<Inner>>> = Mutex::new(None);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeScope {
    #[default]
    Process,
    System,
}

#[derive(Debug, Default, Clone)]
pub struct RuntimeOptions {
    pub scope: RuntimeScope,
    pub endpoint: Option<PathBuf>,
}

impl Inner {
    /// Remove the endpoint file.
    fn remove_endpoint(&self) {
        if let Some(path) = &self.endpoint {
            if fs::symlink_metadata(path)
                .ok()
                .is_some_and(|m| Some((m.dev(), m.ino())) == self.endpoint_identity)
            {
                let _ = fs::remove_file(path);
            }
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.remove_endpoint();
    }
}

pub(crate) struct Publication {
    file: File,
    path: PathBuf,
}

impl Drop for Publication {
    fn drop(&mut self) {
        if self
            .file
            .metadata()
            .ok()
            .zip(fs::symlink_metadata(&self.path).ok())
            .is_some_and(|(a, b)| a.ino() == b.ino() && a.dev() == b.dev())
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) struct Inner {
    pub coordinator: Arc<Coordinator>,
    pub stopping: AtomicBool,
    pub accepting: AtomicBool,
    pub sessions: crate::ipc::Sessions,
    publication: Mutex<Option<Publication>>,
    pub endpoint: Option<PathBuf>,
    endpoint_identity: Option<(u64, u64)>,
}

/// Owns the authoritative backend independently of predictor-loading libraries.
/// Explicit shutdown returns BUSY until engines drain. Dropping a busy runtime
/// retires it but keeps the backend published/pinned; it never revokes leases.
pub struct Runtime {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) fn active_coordinator() -> Option<Arc<Coordinator>> {
    ACTIVE
        .lock()
        .ok()?
        .as_ref()
        .filter(|r| r.accepting.load(Ordering::Acquire))
        .map(|r| r.coordinator.clone())
}

pub(crate) fn private_directory() -> std::result::Result<PathBuf, Status> {
    let uid = unsafe { getuid() };
    // Match the C++ client on macOS as well as Linux; do not use per-shell TMPDIR.
    let directory = PathBuf::from(format!("/tmp/corby-{uid}"));
    match fs::DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(Status::Unavailable),
    }
    let metadata = fs::symlink_metadata(&directory).map_err(|_| Status::Unavailable)?;
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
        return Err(Status::Unavailable);
    }
    Ok(directory)
}

impl Runtime {
    /// Start the runtime.
    pub fn start(options: RuntimeOptions) -> Result<Self> {
        if options.scope == RuntimeScope::Process && options.endpoint.is_some() {
            return Err(Status::InvalidArgument.into());
        }
        let mut active = ACTIVE.lock().map_err(|_| Error(Status::Internal))?;
        if active.is_some() {
            return Err(Status::Conflict.into());
        }
        let directory = private_directory()?;
        let path = directory.join(format!("process-{}.provider", std::process::id()));
        if fs::symlink_metadata(&path).is_ok_and(|m| !m.is_file() || m.uid() != unsafe { getuid() })
        {
            return Err(Status::Unavailable.into());
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|_| Error(Status::Unavailable))?;
        if unsafe { flock(file.as_raw_fd(), 2 | 4) } != 0 {
            return Err(Status::Conflict.into());
        }
        let publication = Publication {
            file: file.try_clone().map_err(|_| Error(Status::Unavailable))?,
            path,
        };
        file.set_len(0).map_err(|_| Error(Status::Unavailable))?;
        let endpoint = if options.scope == RuntimeScope::System {
            Some(
                options
                    .endpoint
                    .unwrap_or_else(|| directory.join("system.sock")),
            )
        } else {
            None
        };
        let listener = match &endpoint {
            Some(path) => {
                let listener = std::os::unix::net::UnixListener::bind(path).map_err(|e| {
                    Error(if e.kind() == std::io::ErrorKind::AddrInUse {
                        Status::Conflict
                    } else {
                        Status::Unavailable
                    })
                })?;
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                    .map_err(|_| Error(Status::Unavailable))?;
                Some(listener)
            }
            None => None,
        };
        let endpoint_identity = endpoint
            .as_ref()
            .and_then(|p| fs::symlink_metadata(p).ok())
            .map(|m| (m.dev(), m.ino()));
        let inner = Arc::new(Inner {
            coordinator: Arc::new(Coordinator::default()),
            stopping: AtomicBool::new(false),
            accepting: AtomicBool::new(true),
            sessions: Default::default(),
            publication: Mutex::new(Some(publication)),
            endpoint,
            endpoint_identity,
        });
        writeln!(
            file,
            "CORBY1 {} {:x}",
            std::process::id(),
            &DISPATCH as *const _ as usize
        )
        .map_err(|_| Error(Status::Unavailable))?;
        file.flush().map_err(|_| Error(Status::Unavailable))?;
        *active = Some(inner.clone());
        if let Some(listener) = listener {
            if let Err(status) = crate::ipc::serve(inner.clone(), listener) {
                inner.stopping.store(true, Ordering::Release);
                *active = None;
                return Err(status.into());
            }
        }
        Ok(Self { inner })
    }

    /// Get the supervisor context.
    pub fn supervisor(&self) -> Context {
        Context(self.inner.coordinator.clone())
    }

    /// Get the context.
    pub fn context(&self) -> Context {
        self.supervisor()
    }

    /// Get the endpoint.
    pub fn endpoint(&self) -> Option<&std::path::Path> {
        self.inner.endpoint.as_deref()
    }

    /// Shutdown the runtime.
    pub fn shutdown(&mut self) -> Result<()> {
        if self.inner.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.coordinator.retire()?;
        if !self.inner.coordinator.lock()?.entries.is_empty() {
            return Err(Status::Busy.into());
        }
        self.inner.stopping.store(true, Ordering::Release);
        // Wake blocking accept before removing the endpoint. New IPC messages
        // are dispatched on arrival, without a periodic accept/sweep tick.
        if let Some(endpoint) = &self.inner.endpoint {
            let _ = std::os::unix::net::UnixStream::connect(endpoint);
        }
        self.inner
            .publication
            .lock()
            .map_err(|_| Error(Status::Internal))?
            .take();
        self.inner.remove_endpoint();
        let mut active = ACTIVE.lock().map_err(|_| Error(Status::Internal))?;
        if active.as_ref().is_some_and(|r| Arc::ptr_eq(r, &self.inner)) {
            *active = None;
        }
        Ok(())
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[repr(C)]
pub struct RuntimeCOptions {
    size: u32,
    abi_version: u32,
    scope: u32,
    reserved: u32,
    endpoint: *const c_char,
}
pub struct RuntimeHandle(Runtime);

#[no_mangle]
pub unsafe extern "C" fn cor_runtime_start(
    options: *const RuntimeCOptions,
    result: *mut *mut RuntimeHandle,
) -> i32 {
    boundary(|| {
        unsafe { crate::output(result)? };
        let mut config = RuntimeOptions::default();
        if !options.is_null() {
            let size = unsafe { prefix(options, 16)? };
            let scope = unsafe { std::ptr::addr_of!((*options).scope).read_unaligned() };
            let reserved = unsafe { std::ptr::addr_of!((*options).reserved).read_unaligned() };
            if reserved != 0 {
                return Err(Status::Unsupported);
            }
            config.scope = match scope {
                0 => RuntimeScope::Process,
                1 => RuntimeScope::System,
                _ => return Err(Status::Unsupported),
            };
            if size
                >= std::mem::offset_of!(RuntimeCOptions, endpoint)
                    + std::mem::size_of::<*const c_char>()
            {
                let path = unsafe { std::ptr::addr_of!((*options).endpoint).read_unaligned() };
                if !path.is_null() {
                    config.endpoint = Some(PathBuf::from(
                        unsafe { CStr::from_ptr(path) }
                            .to_str()
                            .map_err(|_| Status::InvalidArgument)?,
                    ));
                }
            }
        }
        let runtime = Runtime::start(config).map_err(|e| e.0)?;
        unsafe { result.write(Box::into_raw(Box::new(RuntimeHandle(runtime)))) };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn cor_runtime_shutdown(runtime: *mut RuntimeHandle) -> i32 {
    boundary(|| {
        let runtime = unsafe { runtime.as_mut() }.ok_or(Status::InvalidArgument)?;
        runtime.0.shutdown().map_err(|e| e.0)
    })
}

#[no_mangle]
pub unsafe extern "C" fn cor_runtime_destroy(runtime: *mut RuntimeHandle) -> i32 {
    boundary(|| {
        if runtime.is_null() {
            return Ok(());
        }
        let status = unsafe { cor_runtime_shutdown(runtime) };
        if status != 0 {
            return Err(if status == Status::Busy as i32 {
                Status::Busy
            } else {
                Status::Internal
            });
        }
        unsafe { drop(Box::from_raw(runtime)) };
        Ok(())
    })
}
