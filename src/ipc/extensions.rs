/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! Optional v1 operations. Completion and reclamation are pushed from state
//! transitions; the legacy yield monitor is not on either notification path.
use super::*;
use crate::scheduler::Requirement;

pub(super) fn execute(session: &Arc<Session>, opcode: u32, d: &mut Decoder<'_>) -> Reply {
    let id = d.u64()?;
    let mut out = Vec::new();
    match (opcode, session.get(id)?) {
        (20, Object::Engine(e)) => {
            let resource = d.u32()? as usize;
            let intent = d.u32()?;
            d.finish()?;
            put64(
                &mut out,
                session.insert(Object::Work(e.begin_ex(resource, intent)?))?,
            )
        }
        (21, Object::Work(w)) => {
            let flags = d.u32()?;
            let timeout = d.u64()?;
            d.finish()?;
            if flags & !1 != 0 {
                return Err(Status::Unsupported);
            }
            let lease = Arc::new(w.acquire_ex(timeout)?);
            if flags == 0 {
                lease.wait()?;
            }
            let id = session.insert(Object::Lease(lease))?;
            put64(&mut out, id);
        }
        (27, Object::Lease(lease)) => {
            d.finish()?;
            let weak = Arc::downgrade(session);
            lease.register_handler(Arc::new(move |status| {
                if let Some(session) = weak.upgrade() {
                    let mut out = Vec::new();
                    put64(&mut out, id);
                    put32(&mut out, status as u32);
                    session.notify(101, out);
                }
            }))?;
        }
        (22, Object::Lease(l)) => {
            d.finish()?;
            l.acquired()?;
        }
        (23, Object::Engine(e)) => {
            let enabled = d.u32()?;
            d.finish()?;
            if enabled > 1 {
                return Err(Status::InvalidArgument);
            }
            let weak = Arc::downgrade(session);
            e.set_memory_handler(if enabled == 0 {
                None
            } else {
                Some(Arc::new(move |request| {
                    if let Some(session) = weak.upgrade() {
                        if let Ok(ticket) = session.insert(Object::Reclaim(request)) {
                            let mut out = Vec::new();
                            put64(&mut out, id);
                            put64(&mut out, ticket);
                            session.notify(102, out);
                        }
                    }
                }))
            })?;
        }
        (24, Object::Lease(l)) => {
            let timeout = d.u64()?;
            let count = d.u32()?;
            if count == 0 || count > 4096 {
                return Err(Status::InvalidArgument);
            }
            let mut requirements = Vec::with_capacity(count as usize);
            for _ in 0..count {
                requirements.push(Requirement {
                    device_id: d.u32()? as i32,
                    reserved: 0,
                    bytes: d.u64()?,
                });
            }
            d.finish()?;
            let weak = Arc::downgrade(session);
            l.reclaim_memory(
                requirements,
                timeout,
                Arc::new(move |p| {
                    if let Some(session) = weak.upgrade() {
                        let mut out = Vec::new();
                        put64(&mut out, id);
                        for word in [p.pending, p.completed, p.failed, p.status as u32] {
                            put32(&mut out, word);
                        }
                        put64(&mut out, p.released_bytes);
                        session.notify(103, out);
                    }
                }),
            )?;
        }
        (25, Object::Reclaim(r)) => {
            d.finish()?;
            let requirements = r.requirements();
            put32(&mut out, requirements.len() as u32);
            for r in requirements {
                put32(&mut out, r.device_id as u32);
                put64(&mut out, r.bytes);
            }
        }
        (26, Object::Reclaim(r)) => {
            let outcome = crate::extensions::status(d.u32()? as i32)?;
            let bytes = d.u64()?;
            d.finish()?;
            r.complete(outcome, bytes)?;
            session.remove(id)?;
        }
        _ => return Err(Status::InvalidArgument),
    }
    Ok(out)
}
