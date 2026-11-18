/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! Callback exclusion is independent of scheduler locks. Closing a handle
//! suppresses queued deliveries; an executing callback makes close BUSY.
use crate::Status;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

type Handler<T> = Arc<dyn Fn(T) + Send + Sync>;

struct State<T> {
    handler: Option<Handler<T>>,
    active: usize,
    closed: bool,
    idle: Vec<Box<dyn FnOnce() + Send>>,
}

pub(crate) struct Callback<T>(Mutex<State<T>>);

impl<T> Default for Callback<T> {
    fn default() -> Self {
        Self(Mutex::new(State {
            handler: None,
            active: 0,
            closed: false,
            idle: Vec::new(),
        }))
    }
}

impl<T> Callback<T> {
    pub fn install(&self, handler: Option<Handler<T>>) -> Result<(), Status> {
        let mut state = self.0.lock().map_err(|_| Status::Internal)?;
        if state.closed {
            return Err(Status::Retired);
        }
        if state.active != 0 {
            return Err(Status::Busy);
        }
        let previous = std::mem::replace(&mut state.handler, handler);
        drop(state);
        drop(previous);
        Ok(())
    }

    pub fn close(&self) -> Result<(), Status> {
        let mut state = self.0.lock().map_err(|_| Status::Internal)?;
        if state.active != 0 {
            return Err(Status::Busy);
        }
        state.closed = true;
        let previous = state.handler.take();
        drop(state);
        drop(previous);
        Ok(())
    }

    pub fn when_idle(&self, effect: Box<dyn FnOnce() + Send>) {
        let Ok(mut state) = self.0.lock() else {
            return;
        };
        if state.active != 0 {
            state.idle.push(effect);
            return;
        }
        drop(state);
        effect();
    }

    pub fn invoke(&self, value: T) -> Result<bool, Status> {
        let handler = {
            let mut state = self.0.lock().map_err(|_| Status::Internal)?;
            if state.closed {
                return Ok(false);
            }
            let Some(handler) = state.handler.clone() else {
                return Ok(false);
            };
            state.active += 1;
            handler
        };
        let result = catch_unwind(AssertUnwindSafe(|| handler(value)));
        let idle = {
            let mut state = self.0.lock().map_err(|_| Status::Internal)?;
            state.active -= 1;
            if state.active == 0 {
                std::mem::take(&mut state.idle)
            } else {
                Vec::new()
            }
        };
        for effect in idle {
            effect();
        }
        result.map(|_| true).map_err(|_| Status::Internal)
    }
}
