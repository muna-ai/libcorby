/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Completion {
    status: Status,
    registered: bool,
    notified: bool,
}

pub(super) struct Inner {
    pub id: u64,
    pub intent: u32,
    pub started: Instant,
    pub deadline: Option<Instant>,
    pub first_attempt: AtomicBool,
    completion: Mutex<Completion>,
    ready: Condvar,
    callback: Arc<Callback<Status>>,
    closed: AtomicBool,
    pub round: Mutex<Option<Arc<memory::Round>>>,
}

impl Inner {

    pub fn status(&self) -> Result<Status, Status> {
        Ok(self.completion.lock().map_err(|_| Status::Internal)?.status)
    }

    pub(super) fn finish(&self, status: Status, effects: &mut Vec<Effect>) {
        let Ok(mut c) = self.completion.lock() else {
            return;
        };
        if c.status != Status::Pending {
            return;
        }
        c.status = status;
        self.ready.notify_all();
        if c.registered && !c.notified {
            c.notified = true;
            let callback = self.callback.clone();
            effects.push(Box::new(move || {
                let _ = callback.invoke(status);
            }));
        }
    }
}

pub(crate) struct Lease {
    pub work: Arc<Work>,
    pub(super) inner: Arc<Inner>,
}

impl Work {

    pub fn acquire_ex(self: &Arc<Self>, timeout_ns: u64) -> Result<Lease, Status> {
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
        if state.cancelled[&self.id] {
            return Err(Status::Cancelled);
        }
        let lane = &state.entries[&self.engine.id].lanes[self.resource];
        if lane.pending.is_some() || lane.held.is_some() {
            return Err(Status::Busy);
        }
        let id = state.id()?;
        let inner = Arc::new(Inner {
            id,
            intent: self.intent,
            started,
            deadline,
            first_attempt: AtomicBool::new(true),
            completion: Mutex::new(Completion {
                status: Status::Pending,
                registered: false,
                notified: false,
            }),
            ready: Condvar::new(),
            callback: Arc::new(Callback::default()),
            closed: AtomicBool::new(false),
            round: Mutex::new(None),
        });
        let floor = state.entries[&self.engine.id]
            .devices
            .iter()
            .filter_map(|d| state.floors.get(&(self.resource, *d)))
            .copied()
            .max()
            .unwrap_or(0);
        let lane = &mut state.entries.get_mut(&self.engine.id).unwrap().lanes[self.resource];
        lane.pass = lane.pass.max(floor);
        lane.pending = Some(Pending {
            work: self.id,
            ticket: id,
            lease: id,
        });
        state.leases.insert(id, Arc::downgrade(&inner));
        let effects = state.advance();
        // Only state transitions happen inline. Keep warning/progress delivery
        // ordered on the event worker, never on an unrelated allocator thread.
        state.effects.extend(effects);
        drop(state);
        coordinator.notify();
        Ok(Lease {
            work: self.clone(),
            inner,
        })
    }
}
impl Lease {

    pub fn acquired(&self) -> Result<(), Status> {
        match self.inner.status()? {
            Status::Ok => Ok(()),
            status => Err(status),
        }
    }

    pub fn wait(&self) -> Result<(), Status> {
        let mut c = self.inner.completion.lock().map_err(|_| Status::Internal)?;
        while c.status == Status::Pending {
            c = self.inner.ready.wait(c).map_err(|_| Status::Internal)?;
        }
        match c.status {
            Status::Ok => Ok(()),
            status => Err(status),
        }
    }

    pub fn register_handler(
        &self,
        handler: Arc<dyn Fn(Status) + Send + Sync>,
    ) -> Result<(), Status> {
        let mut c = self.inner.completion.lock().map_err(|_| Status::Internal)?;
        if c.registered {
            return Err(Status::Busy);
        }
        self.inner.callback.install(Some(handler))?;
        c.registered = true;
        let status = c.status;
        if status == Status::Pending {
            return Ok(());
        }
        c.notified = true;
        drop(c);
        self.inner.callback.invoke(status)?;
        Ok(())
    }

    pub fn should_yield(&self, min_hold_ns: u64) -> Result<bool, Status> {
        self.acquired()?;
        if self.work.resource != COMPUTE {
            return Err(Status::Unsupported);
        }
        let state = self.work.engine.coordinator.lock()?;
        let id = self.work.engine.id;
        let entry = &state.entries[&id];
        if state.cancelled[&self.work.id] || state.blocked_by_priority(id, COMPUTE) {
            return Ok(true);
        }
        let held = entry.lanes[COMPUTE].held.as_ref().ok_or(Status::Internal)?;
        let elapsed = ns(held.started.elapsed());
        if elapsed < min_hold_ns {
            return Ok(false);
        }
        let projected = entry.lanes[COMPUTE]
            .pass
            .saturating_add(elapsed as u128 * SCALE / held.weight as u128);
        Ok(state.entries.iter().any(|(&other_id, other)| {
            other_id != id
                && other.lanes[COMPUTE]
                    .pending
                    .is_some_and(|p| !state.cancelled[&p.work])
                && entry.overlaps(other)
                && state.admissible(other_id, COMPUTE, Some(id))
                && other.lanes[COMPUTE].pass <= projected
        }))
    }

    pub fn close(&self) -> Result<(), Status> {
        self.inner.callback.close()?;
        if let Some(round) = self
            .inner
            .round
            .lock()
            .map_err(|_| Status::Internal)?
            .as_ref()
        {
            round.close()?;
        }
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let coordinator = &self.work.engine.coordinator;
        let mut state = coordinator.lock()?;
        if let Some(e) = state.entries.get_mut(&self.work.engine.id) {
            let lane = &mut e.lanes[self.work.resource];
            if lane.pending.is_some_and(|p| p.lease == self.inner.id) {
                lane.pending = None;
            }
            if lane.held.as_ref().is_some_and(|h| h.lease == self.inner.id) {
                let held = lane.held.take().unwrap();
                let elapsed = ns(held.started.elapsed());
                if self.work.resource == COMPUTE {
                    e.held_ns = e.held_ns.saturating_add(elapsed);
                }
                // Memory arbitration charges equal turns, not allocation call
                // duration. Compute keeps the original measured-time policy.
                lane.pass = if self.work.resource == COMPUTE {
                    lane.pass
                        .saturating_add(elapsed as u128 * SCALE / held.weight as u128)
                } else {
                    lane.pass.saturating_add(SCALE)
                };
            }
        }
        self.inner.finish(Status::Cancelled, &mut Vec::new());
        state.leases.remove(&self.inner.id);
        drop(state);
        coordinator.notify();
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.close() != Err(Status::Busy) {
            return;
        }
        let deferred = Lease {
            work: self.work.clone(),
            inner: self.inner.clone(),
        };
        let callback = self.inner.callback.clone();
        let round = self.inner.round.lock().ok().and_then(|r| r.clone());
        callback.when_idle(Box::new(move || {
            if let Some(round) = round {
                round.callback.when_idle(Box::new(move || drop(deferred)));
            } else {
                drop(deferred);
            }
        }));
    }
}
