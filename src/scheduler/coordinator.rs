/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

use super::*;

impl Coordinator {
    fn start(&self) -> Result<(), Status> {
        // get_or_init cannot propagate a spawn failure, so serialize its rare
        // initialization with state and publish only a successfully spawned worker.
        let _state = self.lock()?;
        if self.worker.get().is_some() {
            return Ok(());
        }
        let shared = self.shared.clone();
        let worker = std::thread::Builder::new()
            .name("corby-events".into())
            .spawn(move || {
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Self::run(&shared)));
                if result.is_err() || shared.state.is_poisoned() {
                    // A poisoned coordinator cannot grant permission, but must
                    // still resolve promises already returned to clients.
                    let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.shutdown = true;
                    state.stopping = true;
                    let mut effects = Vec::new();
                    for lease in state.leases.values().filter_map(Weak::upgrade) {
                        lease.finish(Status::Internal, &mut effects);
                    }
                    for round in state.rounds.values().filter_map(Weak::upgrade) {
                        round.fail(Status::Internal, &mut effects);
                    }
                    drop(state);
                    shared.changed.notify_all();
                    for effect in effects {
                        effect();
                    }
                }
            })
            .map_err(|_| Status::Unavailable)?;
        self.worker.set(worker).map_err(|_| Status::Internal)
    }
    fn run(shared: &Shared) {
        let Ok(mut state) = shared.state.lock() else {
            return;
        };
        loop {
            if state.shutdown {
                break;
            }
            let effects = state.advance();
            if !effects.is_empty() {
                drop(state);
                for effect in effects {
                    effect();
                }
                let Ok(next) = shared.state.lock() else {
                    return;
                };
                state = next;
                continue;
            }
            let deadline = state.deadline();
            let result = match deadline {
                Some(d) => shared
                    .changed
                    .wait_timeout(state, d.saturating_duration_since(Instant::now()))
                    .map(|r| r.0)
                    .map_err(|_| Status::Internal),
                None => shared.changed.wait(state).map_err(|_| Status::Internal),
            };
            let Ok(next) = result else {
                return;
            };
            state = next;
        }
    }
    pub fn lock(&self) -> Result<MutexGuard<'_, State>, Status> {
        let state = self.shared.state.lock().map_err(|_| Status::Internal)?;
        if state.shutdown {
            return Err(Status::Internal);
        }
        Ok(state)
    }
    pub(super) fn enqueue(&self, effects: Vec<Effect>) -> Result<(), Status> {
        self.lock()?.effects.extend(effects);
        self.notify();
        Ok(())
    }
    pub fn notify(&self) {
        self.shared.changed.notify_all();
    }
    pub fn retire(&self) -> Result<(), Status> {
        let mut state = self.lock()?;
        state.stopping = true;
        for e in state.entries.values_mut() {
            e.retiring = true;
        }
        self.notify();
        Ok(())
    }
    pub fn cancel_all(&self) {
        if let Ok(mut s) = self.lock() {
            for v in s.cancelled.values_mut() {
                *v = true;
            }
        }
        self.notify();
    }
    pub fn register(
        self: &Arc<Self>,
        label: String,
        mut devices: Vec<i32>,
        weight: u32,
        tenancy: u32,
        capabilities: u64,
    ) -> Result<Arc<Engine>, Status> {
        self.start()?;
        devices.sort_unstable();
        if devices.is_empty()
            || devices.iter().any(|&d| d < 0)
            || devices.windows(2).any(|p| p[0] == p[1])
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
        let engine = Arc::new(Engine {
            coordinator: self.clone(),
            id,
            memory_handler: Callback::default(),
            reclaims: 0.into(),
        });
        state.entries.insert(
            id,
            Entry {
                engine: Arc::downgrade(&engine),
                label,
                devices,
                weight,
                tenancy,
                capabilities,
                retiring: false,
                closing: false,
                works: 0,
                grants: 0,
                held_ns: 0,
                waited_ns: 0,
                lanes: Default::default(),
                initializing: 0,
            },
        );
        Ok(engine)
    }
}
impl Drop for Coordinator {
    fn drop(&mut self) {
        if let Ok(mut s) = self.shared.state.lock() {
            s.shutdown = true;
        }
        self.notify();
        if let Some(worker) = self.worker.take() {
            if worker.thread().id() != std::thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}
