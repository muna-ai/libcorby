/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

use super::*;

impl Engine {
    pub fn begin(self: &Arc<Self>) -> Result<Arc<Work>, Status> {
        self.begin_ex(COMPUTE, 0)
    }
    pub fn begin_ex(self: &Arc<Self>, resource: usize, intent: u32) -> Result<Arc<Work>, Status> {
        if resource > MEMORY || intent > OPPORTUNISTIC || (resource == COMPUTE && intent != 0) {
            return Err(Status::InvalidArgument);
        }
        let mut s = self.coordinator.lock()?;
        if s.entries[&self.id].retiring {
            return Err(Status::Retired);
        }
        let id = s.id()?;
        let e = s.entries.get_mut(&self.id).unwrap();
        e.works += 1;
        e.lanes[resource].works += 1;
        if resource == MEMORY && intent == INITIALIZATION {
            e.initializing += 1;
        }
        s.cancelled.insert(id, false);
        self.coordinator.notify();
        Ok(Arc::new(Work {
            engine: self.clone(),
            id,
            resource,
            intent,
        }))
    }
    pub fn unregister(&self) -> Result<(), Status> {
        // Snapshotting recipients uses the same lock. Once closing is set, no
        // warning can borrow a C callback context after successful unregister.
        let mut state = self.coordinator.lock()?;
        let entry = state.entries.get_mut(&self.id).ok_or(Status::Retired)?;
        if entry.works != 0 || self.reclaims.load(std::sync::atomic::Ordering::Acquire) != 0 {
            return Err(Status::Busy);
        }
        entry.closing = true;
        entry.retiring = true;
        drop(state);
        // Every queued/active warning owns a counted ticket, so zero reclaims
        // plus closing fences delivery. Drop callback captures outside state.
        self.memory_handler.close()
    }
    pub fn retire(&self) -> Result<(), Status> {
        self.coordinator
            .lock()?
            .entries
            .get_mut(&self.id)
            .ok_or(Status::Internal)?
            .retiring = true;
        self.coordinator.notify();
        Ok(())
    }
    #[cfg(test)]
    pub fn idle(&self) -> Result<bool, Status> {
        Ok(self.coordinator.lock()?.entries[&self.id].works == 0
            && self.reclaims.load(std::sync::atomic::Ordering::Acquire) == 0)
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
        self.coordinator.notify();
        Ok(())
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        if let Ok(mut s) = self.coordinator.lock() {
            s.entries.remove(&self.id);
        }
        self.coordinator.notify();
    }
}
impl Work {
    pub fn cancel(&self) -> Result<(), Status> {
        *self
            .engine
            .coordinator
            .lock()?
            .cancelled
            .get_mut(&self.id)
            .ok_or(Status::Internal)? = true;
        self.engine.coordinator.notify();
        Ok(())
    }
    pub fn idle(&self) -> Result<bool, Status> {
        let s = self.engine.coordinator.lock()?;
        let lane = &s.entries[&self.engine.id].lanes[self.resource];
        Ok(lane.pending.is_none_or(|p| p.work != self.id)
            && lane.held.as_ref().is_none_or(|h| h.work != self.id))
    }
    pub fn acquire(self: &Arc<Self>, timeout_ns: u64) -> Result<Lease, Status> {
        let lease = self.acquire_ex(timeout_ns)?;
        lease.wait()?;
        Ok(lease)
    }
}
impl Drop for Work {
    fn drop(&mut self) {
        if let Ok(mut s) = self.engine.coordinator.lock() {
            if let Some(e) = s.entries.get_mut(&self.engine.id) {
                e.works -= 1;
                e.lanes[self.resource].works -= 1;
                if self.resource == MEMORY && self.intent == INITIALIZATION {
                    e.initializing -= 1;
                }
            }
            s.cancelled.remove(&self.id);
        }
        self.engine.coordinator.notify();
    }
}
