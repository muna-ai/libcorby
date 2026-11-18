/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

use crate::scheduler;
use crate::{EngineStats, Status, CAP_COOPERATIVE};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub Status);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Corby {:?}", self.0)
    }
}
impl std::error::Error for Error {}
impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Self(status)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug)]
pub enum Tenancy {
    Shared,
    Dedicated,
}

#[derive(Clone)]
pub struct Context(pub(crate) Arc<scheduler::Coordinator>);
pub struct Engine(pub(crate) Arc<scheduler::Engine>);
pub struct Work<'engine>(Arc<scheduler::Work>, PhantomData<&'engine Engine>);
pub struct Lease<'work>(scheduler::Lease, PhantomData<&'work ()>);

#[derive(Clone, Copy, Debug, Default)]
pub enum Resource {
    #[default]
    Compute,
    Memory,
}
#[derive(Clone, Copy, Debug, Default)]
pub enum WorkIntent {
    #[default]
    Required,
    Initialization,
    Opportunistic,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct AcquisitionOptions {
    pub timeout: Option<Duration>,
    pub asynchronous: bool,
}
/// A recipient's completion obligation. Move this onto the engine's own
/// execution thread, release safe-to-reclaim backing, then complete it.
/// Dropping an uncompleted request acknowledges cancellation, never success.
pub struct MemoryRequest(Arc<scheduler::Reclaim>);

fn timeout_ns(timeout: Option<Duration>) -> Result<u64> {
    match timeout {
        None => Ok(u64::MAX),
        Some(d) => u64::try_from(d.as_nanos())
            .ok()
            .filter(|&n| n < u64::MAX)
            .ok_or_else(|| Status::InvalidArgument.into()),
    }
}

impl Context {
    /// Register a new engine with the context.
    pub fn register(
        &self,
        label: &str,
        devices: &[i32],
        weight: u32,
        tenancy: Tenancy,
        cooperative: bool,
    ) -> Result<Engine> {
        if label.is_empty() || label.len() > 4096 || label.contains('\0') {
            return Err(Status::InvalidArgument.into());
        }
        Ok(Engine(self.0.register(
            label.into(),
            devices.into(),
            weight,
            u32::from(matches!(tenancy, Tenancy::Dedicated)),
            if cooperative { CAP_COOPERATIVE } else { 0 },
        )?))
    }

    /// Hold invocation-wide intent on an existing predictor registration.
    /// The work owns the engine internally and may outlive this context.
    pub fn begin_engine(&self, id: u64) -> Result<Work<'static>> {
        let engine = self
            .0
            .lock()?
            .entries
            .get(&id)
            .and_then(|entry| entry.engine.upgrade())
            .ok_or(Status::NotFound)?;
        Ok(Work(engine.begin()?, PhantomData))
    }

    /// Set host policy before admitting invocations. A tenancy change requires
    /// an idle engine and validates the entire overlapping device group.
    pub fn set_engine_tenancy(&self, id: u64, tenancy: Tenancy) -> Result<()> {
        let tenancy = u32::from(matches!(tenancy, Tenancy::Dedicated));
        let mut state = self.0.lock()?;
        let entry = state.entries.get(&id).ok_or(Status::NotFound)?;
        if entry.retiring {
            return Err(Status::Retired.into());
        }
        if entry.tenancy == tenancy {
            return Ok(());
        }
        if entry.works != 0 {
            return Err(Status::Busy.into());
        }
        if tenancy == crate::TENANCY_DEDICATED {
            if entry.capabilities & CAP_COOPERATIVE == 0 {
                return Err(Status::Unsupported.into());
            }
            for (&peer_id, peer) in &state.entries {
                if peer_id == id || !peer.devices.iter().any(|d| entry.devices.contains(d)) {
                    continue;
                }
                if peer.tenancy == crate::TENANCY_DEDICATED {
                    return Err(Status::Conflict.into());
                }
                if peer.capabilities & CAP_COOPERATIVE == 0 {
                    return Err(Status::Unsupported.into());
                }
            }
        }
        state.entries.get_mut(&id).ok_or(Status::NotFound)?.tenancy = tenancy;
        self.0.notify();
        Ok(())
    }

    /// Get all engine IDs registered with the context.
    pub fn engine_ids(&self) -> Result<Vec<u64>> {
        Ok(self.0.lock()?.entries.keys().copied().collect())
    }

    /// Get the stats for an engine.
    pub fn engine_stats(&self, id: u64) -> Result<EngineStats> {
        Ok(self
            .0
            .lock()?
            .entries
            .get(&id)
            .ok_or(Status::NotFound)?
            .stats())
    }

    /// Set the weight for an engine.
    pub fn set_engine_weight(&self, id: u64, weight: u32) -> Result<()> {
        if weight == 0 {
            return Err(Status::InvalidArgument.into());
        }
        self.0
            .lock()?
            .entries
            .get_mut(&id)
            .ok_or(Status::NotFound)?
            .weight = weight;
        self.0.notify();
        Ok(())
    }

    /// Get the label for an engine.
    pub fn engine_label(&self, id: u64) -> Result<String> {
        Ok(self
            .0
            .lock()?
            .entries
            .get(&id)
            .ok_or(Status::NotFound)?
            .label
            .clone())
    }

    /// Get the devices for an engine.
    pub fn engine_devices(&self, id: u64) -> Result<Vec<i32>> {
        Ok(self
            .0
            .lock()?
            .entries
            .get(&id)
            .ok_or(Status::NotFound)?
            .devices
            .clone())
    }
}

impl Engine {
    /// Get the ID of the engine.
    pub fn id(&self) -> u64 {
        self.0.id
    }

    /// Begin a new work item for the engine.
    pub fn begin(&self) -> Result<Work<'_>> {
        Ok(Work(self.0.begin()?, PhantomData))
    }

    /// Resource domains have independent exclusion. Initialization intent
    /// suppresses opportunistic regrowth between this work's memory leases.
    pub fn begin_ex(&self, resource: Resource, intent: WorkIntent) -> Result<Work<'_>> {
        Ok(Work(
            self.0.begin_ex(resource as usize, intent as u32)?,
            PhantomData,
        ))
    }

    /// The callback is a notification: enqueue the request and return promptly.
    /// Replacing/removing an executing handler returns Busy.
    pub fn set_memory_handler<F>(&self, handler: F) -> Result<()>
    where
        F: Fn(MemoryRequest) + Send + Sync + 'static,
    {
        Ok(self
            .0
            .set_memory_handler(Some(Arc::new(move |r| handler(MemoryRequest(r)))))?)
    }
    pub fn clear_memory_handler(&self) -> Result<()> {
        Ok(self.0.set_memory_handler(None)?)
    }

    /// Retire the engine.
    pub fn retire(&self) -> Result<()> {
        Ok(self.0.retire()?)
    }

    /// Set the weight for the engine.
    pub fn set_weight(&self, weight: u32) -> Result<()> {
        Ok(self.0.set_weight(weight)?)
    }

    /// Get the stats for the engine.
    pub fn stats(&self) -> Result<EngineStats> {
        Ok(self.0.coordinator.lock()?.entries[&self.0.id].stats())
    }
}

impl Work<'_> {
    /// Acquire a lease for the work item.
    pub fn acquire(&self, timeout: Option<Duration>) -> Result<Lease<'_>> {
        let ns = timeout_ns(timeout)?;
        Ok(Lease(self.0.acquire(ns)?, PhantomData))
    }
    /// An async result is an owned pending lease, not permission to allocate
    /// or compute. Check acquired() or install a completion handler first.
    pub fn acquire_ex(&self, options: AcquisitionOptions) -> Result<Lease<'_>> {
        let lease = self.0.acquire_ex(timeout_ns(options.timeout)?)?;
        if !options.asynchronous {
            lease.wait()?;
        }
        Ok(Lease(lease, PhantomData))
    }
    pub fn cancel(&self) -> Result<()> {
        Ok(self.0.cancel()?)
    }
}

impl Lease<'_> {
    pub fn acquired(&self) -> Result<()> {
        Ok(self.0.acquired()?)
    }
    pub fn wait(&self) -> Result<()> {
        Ok(self.0.wait()?)
    }
    /// Exactly once, including failures. A completed lease may invoke inline.
    /// Callbacks run outside scheduler locks and should enqueue work promptly.
    pub fn register_handler<F>(&self, handler: F) -> Result<()>
    where
        F: Fn(Status) + Send + Sync + 'static,
    {
        Ok(self.0.register_handler(Arc::new(handler))?)
    }
    pub fn reclaim_memory<F>(
        &self,
        requirements: &[crate::MemoryRequirement],
        timeout: Option<Duration>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(crate::MemoryProgress) + Send + Sync + 'static,
    {
        Ok(self.0.reclaim_memory(
            requirements.to_vec(),
            timeout_ns(timeout)?,
            Arc::new(handler),
        )?)
    }
    /// Check if the lease should yield.
    pub fn should_yield(&self, min_hold: Duration) -> Result<bool> {
        Ok(self
            .0
            .should_yield(min_hold.as_nanos().min(u64::MAX as u128) as u64)?)
    }
}

impl MemoryRequest {
    pub fn requirements(&self) -> Vec<crate::MemoryRequirement> {
        self.0.requirements()
    }
    /// Report only after device work is synchronized and backing is released.
    /// Byte counts are telemetry, not a guarantee that a subsequent allocation succeeds.
    pub fn complete(self, outcome: Status, released_bytes: u64) -> Result<()> {
        Ok(self.0.complete(outcome, released_bytes)?)
    }
}
