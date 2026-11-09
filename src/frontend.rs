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
        let ns = match timeout {
            None => u64::MAX,
            Some(d) => u64::try_from(d.as_nanos())
                .ok()
                .filter(|&n| n < u64::MAX)
                .ok_or(Status::InvalidArgument)?,
        };
        Ok(Lease(self.0.acquire(ns)?, PhantomData))
    }
    pub fn cancel(&self) -> Result<()> {
        Ok(self.0.cancel()?)
    }
}

impl Lease<'_> {
    /// Check if the lease should yield.
    pub fn should_yield(&self, min_hold: Duration) -> Result<bool> {
        Ok(self
            .0
            .should_yield(min_hold.as_nanos().min(u64::MAX as u128) as u64)?)
    }
}
