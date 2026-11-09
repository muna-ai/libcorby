/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! Private scheduling state. No layout in this module crosses the C boundary.
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::{Status, CAP_COOPERATIVE, TENANCY_DEDICATED};

const SCALE: u128 = 1024;

#[derive(Clone, Copy)]
struct Pending {
    work: u64,
    ticket: u64,
}

struct Held {
    work: u64,
    started: Instant,
    weight: u32,
}

pub(crate) struct Entry {
    pub label: String,
    pub devices: Vec<i32>,
    pub weight: u32,
    pub tenancy: u32,
    pub capabilities: u64,
    pub retiring: bool,
    pub works: u64,
    pub grants: u64,
    pub held_ns: u64,
    pub waited_ns: u64,
    pass: u128,
    pending: Option<Pending>,
    held: Option<Held>,
}

impl Entry {

    pub fn stats(&self) -> crate::EngineStats {
        crate::EngineStats {
            size: std::mem::size_of::<crate::EngineStats>() as u32,
            abi_version: crate::ABI_VERSION,
            active_work: self.works,
            grants: self.grants,
            held_ns: self.held_ns,
            waited_ns: self.waited_ns,
            waiting: u32::from(self.pending.is_some()),
            holding: u32::from(self.held.is_some()),
            weight: self.weight,
            tenancy: self.tenancy,
            capabilities: self.capabilities,
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.devices.iter().any(|d| other.devices.contains(d))
    }
}

#[derive(Default)]
pub(crate) struct State {
    next: u64,
    pub entries: BTreeMap<u64, Entry>,
    floors: BTreeMap<i32, u128>,
    cancelled: BTreeMap<u64, bool>,
    stopping: bool,
}

impl State {
    fn id(&mut self) -> Result<u64, Status> {
        self.next = self.next.checked_add(1).ok_or(Status::Internal)?;
        Ok(self.next)
    }

    fn blocked_by_priority(&self, id: u64) -> bool {
        let entry = &self.entries[&id];
        self.entries.iter().any(|(&other_id, other)| {
            other_id != id
                && other.tenancy == TENANCY_DEDICATED
                && other.works > 0
                && entry.overlaps(other)
        })
    }

    fn available(&self, id: u64, ignore_holder: Option<u64>) -> bool {
        let entry = &self.entries[&id];
        !self.blocked_by_priority(id)
            && self.entries.iter().all(|(&other_id, other)| {
                Some(other_id) == ignore_holder || other.held.is_none() || !entry.overlaps(other)
            })
    }

    fn winner(&self) -> Option<u64> {
        self.entries
            .iter()
            .filter_map(|(&id, entry)| {
                let pending = entry.pending?;
                (self.admissible(id, None) && !self.cancelled[&pending.work])
                    .then_some(((entry.pass, pending.ticket), id))
            })
            .min_by_key(|(key, _)| *key)
            .map(|(_, id)| id)
    }

    fn admissible(&self, id: u64, ignore_holder: Option<u64>) -> bool {
        if !self.available(id, ignore_holder) {
            return false;
        }
        let entry = &self.entries[&id];
        let Some(pending) = entry.pending else {
            return false;
        };
        let key = (entry.pass, pending.ticket);
        // Reserve queue precedence, never a physical subset. Without this,
        // staggered single-device arrivals can starve a multi-device group
        // forever by keeping at least one of its devices occupied.
        !self.entries.iter().any(|(&other_id, other)| {
            other_id != id
                && other
                    .pending
                    .is_some_and(|p| !self.cancelled[&p.work] && (other.pass, p.ticket) < key)
                && !self.blocked_by_priority(other_id)
                && entry.overlaps(other)
        })
    }
}

#[derive(Default)]
pub(crate) struct Coordinator {
    state: Mutex<State>,
    changed: Condvar,
}

pub(crate) struct Engine {
    pub coordinator: Arc<Coordinator>,
    pub id: u64,
}

pub(crate) struct Work {
    pub engine: Arc<Engine>,
    pub id: u64,
}

pub(crate) struct Lease {
    pub work: Arc<Work>,
}

fn ns(elapsed: Duration) -> u64 {
    elapsed.as_nanos().min(u64::MAX as u128) as u64
}

impl Coordinator {
    pub fn retire(&self) -> Result<(), Status> {
        let mut state = self.lock()?;
        state.stopping = true;
        for entry in state.entries.values_mut() {
            entry.retiring = true;
        }
        self.changed.notify_all();
        Ok(())
    }

    pub fn cancel_all(&self) {
        if let Ok(mut state) = self.lock() {
            for cancelled in state.cancelled.values_mut() {
                *cancelled = true;
            }
            self.changed.notify_all();
        }
    }

    pub fn notify(&self) {
        self.changed.notify_all();
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, State>, Status> {
        self.state.lock().map_err(|_| Status::Internal)
    }

    pub fn register(
        self: &Arc<Self>,
        label: String,
        mut devices: Vec<i32>,
        weight: u32,
        tenancy: u32,
        capabilities: u64,
    ) -> Result<Arc<Engine>, Status> {
        devices.sort_unstable();
        if devices.is_empty()
            || devices.iter().any(|&d| d < 0)
            || devices.windows(2).any(|pair| pair[0] == pair[1])
            || weight == 0
            || tenancy > TENANCY_DEDICATED
        {
            return Err(Status::InvalidArgument);
        }
        let mut state = self.lock()?;
        if state.stopping {
            return Err(Status::Retired);
        }
        if state.entries.len() >= 4096 {
            return Err(Status::Busy);
        }
        for other in state.entries.values() {
            if !devices.iter().any(|d| other.devices.contains(d)) {
                continue;
            }
            if tenancy == TENANCY_DEDICATED && other.tenancy == TENANCY_DEDICATED {
                return Err(Status::Conflict);
            }
            if (tenancy == TENANCY_DEDICATED || other.tenancy == TENANCY_DEDICATED)
                && (capabilities & other.capabilities & CAP_COOPERATIVE == 0)
            {
                return Err(Status::Unsupported);
            }
        }
        if tenancy == TENANCY_DEDICATED && capabilities & CAP_COOPERATIVE == 0 {
            return Err(Status::Unsupported);
        }
        let id = state.id()?;
        state.entries.insert(
            id,
            Entry {
                label,
                devices,
                weight,
                tenancy,
                capabilities,
                retiring: false,
                works: 0,
                grants: 0,
                held_ns: 0,
                waited_ns: 0,
                pass: 0,
                pending: None,
                held: None,
            },
        );
        Ok(Arc::new(Engine {
            coordinator: self.clone(),
            id,
        }))
    }
}

impl Engine {
    pub fn begin(self: &Arc<Self>) -> Result<Arc<Work>, Status> {
        let mut state = self.coordinator.lock()?;
        if state.entries[&self.id].retiring {
            return Err(Status::Retired);
        }
        let id = state.id()?;
        let entry = state.entries.get_mut(&self.id).ok_or(Status::Internal)?;
        entry.works = entry.works.checked_add(1).ok_or(Status::Internal)?;
        state.cancelled.insert(id, false);
        self.coordinator.changed.notify_all();
        Ok(Arc::new(Work {
            engine: self.clone(),
            id,
        }))
    }

    pub fn retire(&self) -> Result<(), Status> {
        self.coordinator
            .lock()?
            .entries
            .get_mut(&self.id)
            .ok_or(Status::Internal)?
            .retiring = true;
        Ok(())
    }

    pub fn idle(&self) -> Result<bool, Status> {
        Ok(self.coordinator.lock()?.entries[&self.id].works == 0)
    }

    pub fn set_weight(&self, weight: u32) -> Result<(), Status> {
        if weight == 0 {
            return Err(Status::InvalidArgument);
        }
        self.coordinator
            .lock()?
            .entries
            .get_mut(&self.id)
            .ok_or(Status::Internal)?
            .weight = weight;
        self.coordinator.changed.notify_all();
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // No work can remain: every Work owns an Arc<Engine>.
        if let Ok(mut state) = self.coordinator.state.lock() {
            state.entries.remove(&self.id);
            self.coordinator.changed.notify_all();
        }
    }
}

impl Work {

    /// Cancel the work item.
    pub fn cancel(&self) -> Result<(), Status> {
        *self
            .engine
            .coordinator
            .lock()?
            .cancelled
            .get_mut(&self.id)
            .ok_or(Status::Internal)? = true;
        self.engine.coordinator.changed.notify_all();
        Ok(())
    }

    /// Check if the work item is idle.
    pub fn idle(&self) -> Result<bool, Status> {
        let state = self.engine.coordinator.lock()?;
        let entry = &state.entries[&self.engine.id];
        Ok(entry.pending.is_none_or(|p| p.work != self.id)
            && entry.held.as_ref().is_none_or(|h| h.work != self.id))
    }

    /// Acquire a lease for the work item.
    pub fn acquire(self: &Arc<Self>, timeout_ns: u64) -> Result<Lease, Status> {
        let coordinator = &self.engine.coordinator;
        let started = Instant::now();
        let deadline = if timeout_ns == u64::MAX {
            None
        } else {
            Some(
                started
                    .checked_add(Duration::from_nanos(timeout_ns))
                    .ok_or(Status::InvalidArgument)?,
            )
        };
        let mut state = coordinator.lock()?;
        let id = self.engine.id;
        if state.cancelled[&self.id] {
            return Err(Status::Cancelled);
        }
        let entry = &state.entries[&id];
        if entry.held.is_some() || entry.pending.is_some() {
            return Err(Status::Busy);
        }
        let floor = entry
            .devices
            .iter()
            .filter_map(|d| state.floors.get(d))
            .copied()
            .max()
            .unwrap_or(0);
        let ticket = state.id()?;
        let entry = state.entries.get_mut(&id).ok_or(Status::Internal)?;
        entry.pass = entry.pass.max(floor);
        entry.pending = Some(Pending {
            work: self.id,
            ticket,
        });
        coordinator.changed.notify_all();
        loop {
            let failure = if state.cancelled[&self.id] {
                Some(Status::Cancelled)
            } else if state.winner() == Some(id) {
                let entry = state.entries.get_mut(&id).ok_or(Status::Internal)?;
                entry.pending = None;
                entry.grants = entry.grants.saturating_add(1);
                entry.waited_ns = entry.waited_ns.saturating_add(ns(started.elapsed()));
                entry.held = Some(Held {
                    work: self.id,
                    started: Instant::now(),
                    weight: entry.weight,
                });
                let pass = entry.pass;
                let devices = entry.devices.clone();
                for device in devices {
                    let floor = state.floors.entry(device).or_default();
                    *floor = (*floor).max(pass);
                }
                coordinator.changed.notify_all();
                return Ok(Lease { work: self.clone() });
            } else if deadline.is_some_and(|d| Instant::now() >= d) {
                Some(Status::Timeout)
            } else {
                None
            };
            if let Some(error) = failure {
                state.entries.get_mut(&id).ok_or(Status::Internal)?.pending = None;
                coordinator.changed.notify_all();
                return Err(error);
            }
            state = match deadline {
                Some(deadline) => {
                    coordinator
                        .changed
                        .wait_timeout(state, deadline.saturating_duration_since(Instant::now()))
                        .map_err(|_| Status::Internal)?
                        .0
                }
                None => coordinator
                    .changed
                    .wait(state)
                    .map_err(|_| Status::Internal)?,
            };
        }
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        if let Ok(mut state) = self.engine.coordinator.state.lock() {
            if let Some(entry) = state.entries.get_mut(&self.engine.id) {
                entry.works -= 1;
            }
            state.cancelled.remove(&self.id);
            self.engine.coordinator.changed.notify_all();
        }
    }
}

impl Lease {

    /// Check if the lease should yield.
    pub fn should_yield(&self, min_hold_ns: u64) -> Result<bool, Status> {
        let coordinator = &self.work.engine.coordinator;
        let state = coordinator.lock()?;
        let id = self.work.engine.id;
        let entry = &state.entries[&id];
        if state.cancelled[&self.work.id] || state.blocked_by_priority(id) {
            return Ok(true);
        }
        let held = entry.held.as_ref().ok_or(Status::Internal)?;
        let elapsed = ns(held.started.elapsed());
        if elapsed < min_hold_ns {
            return Ok(false);
        }
        let projected = entry
            .pass
            .saturating_add(elapsed as u128 * SCALE / held.weight as u128);
        Ok(state.entries.iter().any(|(&other_id, other)| {
            other_id != id
                && other.pending.is_some_and(|p| !state.cancelled[&p.work])
                && entry.overlaps(other)
                && state.admissible(other_id, Some(id))
                && other.pass <= projected
        }))
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let coordinator = &self.work.engine.coordinator;
        if let Ok(mut state) = coordinator.state.lock() {
            if let Some(entry) = state.entries.get_mut(&self.work.engine.id) {
                if let Some(held) = entry.held.take() {
                    let elapsed = ns(held.started.elapsed());
                    entry.held_ns = entry.held_ns.saturating_add(elapsed);
                    entry.pass = entry
                        .pass
                        .saturating_add(elapsed as u128 * SCALE / held.weight as u128);
                }
            }
            coordinator.changed.notify_all();
        }
    }
}

#[cfg(test)]
mod fairness_tests {
    use super::*;

    #[test]
    fn held_time_weights_and_idle_return_floor() {
        let c = Arc::new(Coordinator::default());
        let a = c.register("a".into(), vec![0], 1, 0, 1).unwrap();
        let b = c.register("b".into(), vec![0], 3, 0, 1).unwrap();
        let wa = a.begin().unwrap();
        let wb = b.begin().unwrap();
        // Synthetic equal-length quanta validate scheduling independently of OS
        // timer granularity and test-runner load. Real drops test accounting below.
        {
            let mut s = c.lock().unwrap();
            s.entries.get_mut(&a.id).unwrap().pending = Some(Pending {
                work: wa.id,
                ticket: 1,
            });
            s.entries.get_mut(&b.id).unwrap().pending = Some(Pending {
                work: wb.id,
                ticket: 2,
            });
            let mut grants = [0; 2];
            for _ in 0..400 {
                let id = s.winner().unwrap();
                grants[usize::from(id == b.id)] += 1;
                let e = s.entries.get_mut(&id).unwrap();
                e.pass += 3_000_000 * SCALE / e.weight as u128;
            }
            assert_eq!(grants, [100, 300]);
            s.entries.get_mut(&a.id).unwrap().pending = None;
            s.entries.get_mut(&b.id).unwrap().pending = None;
            s.floors.insert(0, 999_999_999_999);
        }
        let held = wa.acquire(0).unwrap();
        assert!(c.lock().unwrap().entries[&a.id].pass >= 999_999_999_999);
        drop(held);
        assert!(c.lock().unwrap().entries[&a.id].held_ns > 0);
    }

    #[test]
    fn poisoned_state_returns_error_instead_of_free_running() {
        let c = Arc::new(Coordinator::default());
        let other = c.clone();
        let _ = std::thread::spawn(move || {
            let _state = other.state.lock().unwrap();
            panic!("poison");
        })
        .join();
        assert!(matches!(c.lock(), Err(Status::Internal)));
    }
}
