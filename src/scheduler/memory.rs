/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! Reclamation tickets are capabilities, not externally visible round IDs.
//! Acknowledgements count completed releases, never mere warning delivery.
use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Requirement {
    pub device_id: i32,
    pub reserved: u32,
    pub bytes: u64,
}

impl Requirement {
    pub fn new(device_id: i32, bytes: u64) -> Self {
        Self {
            device_id,
            reserved: 0,
            bytes,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MemoryProgress {
    pub size: u32,
    pub abi_version: u32,
    pub pending: u32,
    pub completed: u32,
    pub failed: u32,
    pub status: i32,
    pub released_bytes: u64,
}

struct Progress {
    pending: BTreeSet<u64>,
    completed: u32,
    failed: u32,
    error: Status,
    released_bytes: u64,
    version: u64,
    published: u64,
    terminal_sent: bool,
    closed: bool,
}

pub(super) struct Round {
    coordinator: Weak<Coordinator>,
    requirements: Vec<Requirement>,
    state: Mutex<Progress>,
    pub(super) callback: Arc<Callback<MemoryProgress>>,
    deadline: Option<Instant>,
}

impl Round {
    pub fn close(&self) -> Result<(), Status> {
        self.callback.close()?;
        self.state.lock().map_err(|_| Status::Internal)?.closed = true;
        Ok(())
    }

    pub fn deadline(&self) -> Option<Instant> {
        let state = self.state.lock().ok()?;
        if state.closed || state.terminal_sent || state.pending.is_empty() {
            None
        } else {
            self.deadline
        }
    }

    pub(super) fn advance(&self, effects: &mut Vec<Effect>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed || state.terminal_sent {
            return;
        }
        let timed_out =
            self.deadline.is_some_and(|d| d <= Instant::now()) && !state.pending.is_empty();
        if !timed_out && state.version == state.published {
            return;
        }
        let status = if timed_out {
            Status::Timeout
        } else if state.pending.is_empty() {
            state.error
        } else {
            Status::Pending
        };
        state.published = state.version;
        state.terminal_sent = status != Status::Pending;
        let progress = MemoryProgress {
            size: std::mem::size_of::<MemoryProgress>() as u32,
            abi_version: crate::ABI_VERSION,
            pending: state.pending.len() as u32,
            completed: state.completed,
            failed: state.failed,
            status: status as i32,
            released_bytes: state.released_bytes,
        };
        let callback = self.callback.clone();
        effects.push(Box::new(move || {
            let _ = callback.invoke(progress);
        }));
    }

    pub(super) fn fail(&self, outcome: Status, effects: &mut Vec<Effect>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed || state.terminal_sent {
            return;
        }
        state.terminal_sent = true;
        let progress = MemoryProgress {
            size: std::mem::size_of::<MemoryProgress>() as u32,
            abi_version: crate::ABI_VERSION,
            pending: state.pending.len() as u32,
            completed: state.completed,
            failed: state.failed,
            status: outcome as i32,
            released_bytes: state.released_bytes,
        };
        let callback = self.callback.clone();
        effects.push(Box::new(move || {
            let _ = callback.invoke(progress);
        }));
    }

    fn complete(&self, engine_id: u64, outcome: Status, bytes: u64) -> Result<(), Status> {
        let mut state = self.state.lock().map_err(|_| Status::Internal)?;
        if !state.pending.remove(&engine_id) {
            return Err(Status::InvalidArgument);
        }
        state.completed += 1;
        if outcome != Status::Ok {
            state.failed += 1;
            if state.error == Status::Ok {
                state.error = outcome;
            }
        }
        state.released_bytes = state.released_bytes.saturating_add(bytes);
        state.version += 1;
        drop(state);
        if let Some(c) = self.coordinator.upgrade() {
            c.notify();
        }
        Ok(())
    }
}

pub(crate) struct Reclaim {
    round: Arc<Round>,
    engine: Arc<Engine>,
    completed: AtomicBool,
}

impl Reclaim {

    pub fn requirements(&self) -> Vec<Requirement> {
        self.round
            .requirements
            .iter()
            .filter(|r| {
                self.engine
                    .coordinator
                    .lock()
                    .is_ok_and(|s| s.entries[&self.engine.id].devices.contains(&r.device_id))
            })
            .copied()
            .collect()
    }

    pub fn complete(&self, outcome: Status, bytes: u64) -> Result<(), Status> {
        if outcome == Status::Pending {
            return Err(Status::InvalidArgument);
        }
        if self.completed.swap(true, Ordering::AcqRel) {
            return Err(Status::InvalidArgument);
        }
        self.round.complete(self.engine.id, outcome, bytes)
    }
}

impl Drop for Reclaim {
    fn drop(&mut self) {
        if !self.completed.swap(true, Ordering::AcqRel) {
            let _ = self.round.complete(self.engine.id, Status::Cancelled, 0);
        }
        self.engine.reclaims.fetch_sub(1, Ordering::AcqRel);
        self.engine.coordinator.notify();
    }
}

impl Engine {

    pub fn set_memory_handler(
        &self,
        handler: Option<Arc<dyn Fn(Arc<Reclaim>) + Send + Sync>>,
    ) -> Result<(), Status> {
        let enabled = handler.is_some();
        self.memory_handler.install(handler)?;
        let mut state = self.coordinator.lock()?;
        let capabilities = &mut state
            .entries
            .get_mut(&self.id)
            .ok_or(Status::Internal)?
            .capabilities;
        if enabled {
            *capabilities |= crate::CAP_MEMORY_RECLAIM;
        } else {
            *capabilities &= !crate::CAP_MEMORY_RECLAIM;
        }
        Ok(())
    }
}

impl Lease {

    pub fn reclaim_memory(
        &self,
        requirements: Vec<Requirement>,
        timeout_ns: u64,
        handler: Arc<dyn Fn(MemoryProgress) + Send + Sync>,
    ) -> Result<(), Status> {
        self.acquired()?;
        if self.work.resource != MEMORY || requirements.is_empty() || requirements.len() > 4096 {
            return Err(Status::InvalidArgument);
        }
        let deadline = if timeout_ns == u64::MAX {
            None
        } else {
            Some(
                Instant::now()
                    .checked_add(Duration::from_nanos(timeout_ns))
                    .ok_or(Status::InvalidArgument)?,
            )
        };
        let coordinator = &self.work.engine.coordinator;
        let mut state = coordinator.lock()?;
        let devices = &state.entries[&self.work.engine.id].devices;
        let mut unique = BTreeSet::new();
        for r in &requirements {
            if r.reserved != 0
                || r.bytes == 0
                || !devices.contains(&r.device_id)
                || !unique.insert(r.device_id)
            {
                return Err(Status::InvalidArgument);
            }
        }
        let mut active = self.inner.round.lock().map_err(|_| Status::Internal)?;
        if active.is_some() {
            return Err(Status::Busy);
        }
        let peers: Vec<_> = state
            .entries
            .iter()
            .filter(|(id, e)| {
                **id != self.work.engine.id
                    && !e.closing
                    && e.devices.iter().any(|d| unique.contains(d))
            })
            .filter_map(|(_, e)| e.engine.upgrade())
            .collect();
        let round = Arc::new(Round {
            coordinator: Arc::downgrade(coordinator),
            requirements,
            state: Mutex::new(Progress {
                pending: peers.iter().map(|e| e.id).collect(),
                completed: 0,
                failed: 0,
                error: Status::Ok,
                released_bytes: 0,
                version: 1,
                published: 0,
                terminal_sent: false,
                closed: false,
            }),
            callback: Arc::new(Callback::default()),
            deadline,
        });
        round.callback.install(Some(handler))?;
        let id = state.id()?;
        state.rounds.insert(id, Arc::downgrade(&round));
        let mut effects: Vec<Effect> = Vec::new();
        for engine in peers {
            engine.reclaims.fetch_add(1, Ordering::AcqRel);
            let request = Arc::new(Reclaim {
                round: round.clone(),
                engine: engine.clone(),
                completed: AtomicBool::new(false),
            });
            effects.push(Box::new(move || {
                if engine.memory_handler.invoke(request.clone()).ok() != Some(true) {
                    let _ = request.complete(Status::Unsupported, 0);
                }
            }));
        }
        *active = Some(round);
        drop(active);
        drop(state);
        // Delivery is queued on the coordinator worker, not performed on the
        // allocator thread. Handlers enqueue on their own executors and return.
        coordinator.enqueue(effects)?;
        Ok(())
    }
}
