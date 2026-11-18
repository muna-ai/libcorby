/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! All resource domains share one event-driven coordinator. The worker sleeps
//! until a state change or the next actual deadline; no acquisition polls.
mod callback;
mod coordinator;
mod engine;
mod lease;
mod memory;
use crate::{EngineStats, Status, CAP_COOPERATIVE, TENANCY_DEDICATED};
use callback::Callback;
pub(crate) use lease::Lease;
pub(crate) use memory::Reclaim;
pub use memory::{MemoryProgress, Requirement};
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(crate) const COMPUTE: usize = 0;
pub(crate) const MEMORY: usize = 1;
pub(crate) const INITIALIZATION: u32 = 1;
pub(crate) const OPPORTUNISTIC: u32 = 2;
const SCALE: u128 = 1024;
type Effect = Box<dyn FnOnce() + Send>;

#[derive(Clone, Copy)]
struct Pending {
    work: u64,
    ticket: u64,
    lease: u64,
}
struct Held {
    work: u64,
    lease: u64,
    started: Instant,
    weight: u32,
}
#[derive(Default)]
struct Lane {
    works: u64,
    pass: u128,
    pending: Option<Pending>,
    held: Option<Held>,
}

pub(crate) struct Entry {
    pub engine: Weak<Engine>,
    pub label: String,
    pub devices: Vec<i32>,
    pub weight: u32,
    pub tenancy: u32,
    pub capabilities: u64,
    pub retiring: bool,
    pub closing: bool,
    pub works: u64,
    pub grants: u64,
    pub held_ns: u64,
    pub waited_ns: u64,
    lanes: [Lane; 2],
    initializing: u64,
}
impl Entry {
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            size: std::mem::size_of::<EngineStats>() as u32,
            abi_version: crate::ABI_VERSION,
            active_work: self.works,
            grants: self.grants,
            held_ns: self.held_ns,
            waited_ns: self.waited_ns,
            // Preserve the v1 execution metrics; memory state is queried on
            // its lease rather than changing old boolean snapshot semantics.
            waiting: u32::from(self.lanes[COMPUTE].pending.is_some()),
            holding: u32::from(self.lanes[COMPUTE].held.is_some()),
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
    floors: BTreeMap<(usize, i32), u128>,
    cancelled: BTreeMap<u64, bool>,
    leases: BTreeMap<u64, Weak<lease::Inner>>,
    rounds: BTreeMap<u64, Weak<memory::Round>>,
    stopping: bool,
    shutdown: bool,
    effects: Vec<Effect>,
}
impl State {
    fn id(&mut self) -> Result<u64, Status> {
        self.next = self.next.checked_add(1).ok_or(Status::Internal)?;
        Ok(self.next)
    }
    fn blocked_by_priority(&self, id: u64, resource: usize) -> bool {
        if resource != COMPUTE {
            return false;
        }
        let entry = &self.entries[&id];
        self.entries.iter().any(|(&other_id, other)| {
            other_id != id
                && other.tenancy == TENANCY_DEDICATED
                && other.lanes[COMPUTE].works > 0
                && entry.overlaps(other)
        })
    }
    fn available(&self, id: u64, resource: usize, ignore: Option<u64>) -> bool {
        let entry = &self.entries[&id];
        !self.blocked_by_priority(id, resource)
            && self.entries.iter().all(|(&other_id, other)| {
                Some(other_id) == ignore
                    || other.lanes[resource].held.is_none()
                    || !entry.overlaps(other)
            })
    }
    fn opportunistic_blocked(&self, id: u64, resource: usize) -> bool {
        if resource != MEMORY {
            return false;
        }
        let entry = &self.entries[&id];
        let Some(pending) = entry.lanes[MEMORY].pending else {
            return false;
        };
        let Some(lease) = self.leases.get(&pending.lease).and_then(Weak::upgrade) else {
            return false;
        };
        lease.intent == OPPORTUNISTIC
            && self.entries.iter().any(|(&other_id, other)| {
                other_id != id
                    && entry.overlaps(other)
                    && (other.initializing > 0
                        || other.lanes[MEMORY].pending.is_some_and(|p| {
                            self.leases
                                .get(&p.lease)
                                .and_then(Weak::upgrade)
                                .is_some_and(|l| l.intent != OPPORTUNISTIC)
                        }))
            })
    }
    fn admissible(&self, id: u64, resource: usize, ignore: Option<u64>) -> bool {
        if !self.available(id, resource, ignore) || self.opportunistic_blocked(id, resource) {
            return false;
        }
        let entry = &self.entries[&id];
        let Some(pending) = entry.lanes[resource].pending else {
            return false;
        };
        let key = (entry.lanes[resource].pass, pending.ticket);
        !self.entries.iter().any(|(&other_id, other)| {
            other_id != id
                && other.lanes[resource].pending.is_some_and(|p| {
                    !self.cancelled[&p.work] && (other.lanes[resource].pass, p.ticket) < key
                })
                && !self.blocked_by_priority(other_id, resource)
                && !self.opportunistic_blocked(other_id, resource)
                && entry.overlaps(other)
        })
    }
    fn winner(&self, resource: usize) -> Option<u64> {
        self.entries
            .iter()
            .filter_map(|(&id, e)| {
                let p = e.lanes[resource].pending?;
                (self.admissible(id, resource, None) && !self.cancelled[&p.work])
                    .then_some(((e.lanes[resource].pass, p.ticket), id))
            })
            .min_by_key(|(key, _)| *key)
            .map(|(_, id)| id)
    }
    fn advance(&mut self) -> Vec<Effect> {
        let mut effects = std::mem::take(&mut self.effects);
        let now = Instant::now();
        // Cancellation precedes granting. Deadlines are checked after a grant
        // attempt so a zero timeout retains try-once behavior.
        for e in self.entries.values_mut() {
            for lane in &mut e.lanes {
                if let Some(p) = lane.pending {
                    let expired = self
                        .leases
                        .get(&p.lease)
                        .and_then(Weak::upgrade)
                        .is_some_and(|l| {
                            let first = l
                                .first_attempt
                                .swap(false, std::sync::atomic::Ordering::AcqRel);
                            !first && l.deadline.is_some_and(|d| d <= now)
                        });
                    if self.cancelled.get(&p.work) == Some(&true) || expired {
                        lane.pending = None;
                        if let Some(l) = self.leases.get(&p.lease).and_then(Weak::upgrade) {
                            l.finish(
                                if expired {
                                    Status::Timeout
                                } else {
                                    Status::Cancelled
                                },
                                &mut effects,
                            );
                        }
                    }
                }
            }
        }
        for resource in [COMPUTE, MEMORY] {
            while let Some(id) = self.winner(resource) {
                let e = self.entries.get_mut(&id).unwrap();
                let p = e.lanes[resource].pending.take().unwrap();
                let Some(l) = self.leases.get(&p.lease).and_then(Weak::upgrade) else {
                    continue;
                };
                e.lanes[resource].held = Some(Held {
                    work: p.work,
                    lease: p.lease,
                    started: now,
                    weight: e.weight,
                });
                if resource == COMPUTE {
                    e.grants = e.grants.saturating_add(1);
                    e.waited_ns = e.waited_ns.saturating_add(ns(l.started.elapsed()));
                }
                for d in &e.devices {
                    let floor = self.floors.entry((resource, *d)).or_default();
                    *floor = (*floor).max(e.lanes[resource].pass);
                }
                l.finish(Status::Ok, &mut effects);
            }
        }
        for e in self.entries.values_mut() {
            for lane in &mut e.lanes {
                if let Some(p) = lane.pending {
                    if let Some(l) = self.leases.get(&p.lease).and_then(Weak::upgrade) {
                        if l.deadline.is_some_and(|d| d <= now) {
                            lane.pending = None;
                            l.finish(Status::Timeout, &mut effects);
                        }
                    }
                }
            }
        }
        self.leases.retain(|_, l| l.strong_count() > 0);
        self.rounds.retain(|_, r| {
            if let Some(round) = r.upgrade() {
                round.advance(&mut effects);
                true
            } else {
                false
            }
        });
        effects
    }
    fn deadline(&self) -> Option<Instant> {
        self.entries
            .values()
            .flat_map(|e| e.lanes.iter())
            .filter_map(|lane| {
                let p = lane.pending?;
                self.leases.get(&p.lease)?.upgrade()?.deadline
            })
            .chain(
                self.rounds
                    .values()
                    .filter_map(Weak::upgrade)
                    .filter_map(|r| r.deadline()),
            )
            .min()
    }
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}
pub(crate) struct Coordinator {
    shared: Arc<Shared>,
    worker: OnceLock<JoinHandle<()>>,
}
impl Default for Coordinator {
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared::default()),
            worker: OnceLock::new(),
        }
    }
}

pub(crate) struct Engine {
    pub coordinator: Arc<Coordinator>,
    pub id: u64,
    pub memory_handler: Callback<Arc<Reclaim>>,
    reclaims: std::sync::atomic::AtomicUsize,
}
pub(crate) struct Work {
    pub engine: Arc<Engine>,
    pub id: u64,
    pub resource: usize,
    pub intent: u32,
}
fn ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests;
