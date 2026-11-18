/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

//! Versioned, bounded, little-endian messages. Mutable scheduler state never
//! crosses the process boundary. Each RPC has its own stream so a pending grant
//! cannot block a release/cancel on the session's other requests.
mod extensions;
use crate::runtime::Inner;
use crate::scheduler::{Engine, Lease, Reclaim, Work};
use crate::{EngineStats, Status, FEATURES};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MAGIC: u32 = 0x434f5242;
const LIMIT: usize = 65536;

fn same_user(stream: &UnixStream) -> bool {
    #[cfg(target_os = "linux")]
    {
        #[repr(C)]
        struct Credentials {
            pid: i32,
            uid: u32,
            gid: u32,
        }
        extern "C" {
            fn getsockopt(
                fd: i32,
                level: i32,
                option: i32,
                value: *mut std::ffi::c_void,
                size: *mut u32,
            ) -> i32;
        }
        let mut credentials = Credentials {
            pid: 0,
            uid: u32::MAX,
            gid: 0,
        };
        let mut size = std::mem::size_of::<Credentials>() as u32;
        unsafe {
            getsockopt(
                stream.as_raw_fd(),
                1,
                17,
                (&mut credentials as *mut Credentials).cast(),
                &mut size,
            ) == 0
                && size as usize == std::mem::size_of::<Credentials>()
                && credentials.uid == crate::runtime::getuid()
        }
    }
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn getpeereid(fd: i32, uid: *mut u32, gid: *mut u32) -> i32;
        }
        let mut uid = u32::MAX;
        let mut gid = 0;
        unsafe {
            getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) == 0
                && uid == crate::runtime::getuid()
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = stream;
        false
    }
}
type Reply = std::result::Result<Vec<u8>, Status>;

#[derive(Default)]
pub(crate) struct Sessions {
    sessions: Mutex<BTreeMap<[u8; 16], Arc<Session>>>,
    workers: AtomicUsize,
}

#[derive(Clone)]
enum Object {
    Engine(Arc<Engine>),
    Work(Arc<Work>),
    Lease(Arc<Lease>),
    Reclaim(Arc<Reclaim>),
}

struct Session {
    objects: Mutex<BTreeMap<u64, Object>>,
    next: AtomicU64,
    closed: AtomicBool,
    notifications: SyncSender<Option<(u32, Vec<u8>)>>,
    control: UnixStream,
}

impl Session {
    fn notify(&self, opcode: u32, body: Vec<u8>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        // Scheduler callbacks only enqueue. A slow/disconnected peer must not
        // stall grants or warning delivery to another engine.
        if self.notifications.try_send(Some((opcode, body))).is_err() {
            self.closed.store(true, Ordering::Release);
            let _ = self.control.shutdown(std::net::Shutdown::Both);
        }
    }

    /// Insert a new object into the session.
    fn insert(&self, object: Object) -> std::result::Result<u64, Status> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err(Status::Internal);
        }
        let mut objects = self.objects.lock().map_err(|_| Status::Internal)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Status::Unavailable);
        }
        objects.insert(id, object);
        Ok(id)
    }

    /// Get an object from the session.
    fn get(&self, id: u64) -> std::result::Result<Object, Status> {
        self.objects
            .lock()
            .map_err(|_| Status::Internal)?
            .get(&id)
            .cloned()
            .ok_or(Status::NotFound)
    }

    /// Remove an object from the session.
    fn remove(&self, id: u64) -> std::result::Result<(), Status> {
        let object = self
            .objects
            .lock()
            .map_err(|_| Status::Internal)?
            .remove(&id);
        drop(object);
        Ok(())
    }

    /// Disconnect the session.
    fn disconnect(&self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.notifications.try_send(None);
        let _ = self.control.shutdown(std::net::Shutdown::Both);
        if let Ok(mut objects) = self.objects.lock() {
            for object in objects.values() {
                if let Object::Work(work) = object {
                    let _ = work.cancel();
                }
            }
            // A lost peer cannot promise reclamation success, even if its
            // compute permission must remain quarantined.
            objects.retain(|_, o| !matches!(o, Object::Reclaim(_)));
            // A broken control connection is not proof of a drained GPU. Keep
            // uncertain holders AND their demand/reservations quarantined.
            for object in objects.values() {
                if let Object::Lease(lease) = object {
                    if lease.acquired() != Ok(()) {
                        let _ = lease.close();
                    }
                }
            }
            if !objects
                .values()
                .any(|o| matches!(o, Object::Lease(l) if l.acquired() == Ok(())))
            {
                objects.clear();
            }
        }
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, count: usize) -> std::result::Result<&'a [u8], Status> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(Status::InvalidArgument)?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or(Status::InvalidArgument)?;
        self.offset = end;
        Ok(result)
    }
    fn u32(&mut self) -> std::result::Result<u32, Status> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> std::result::Result<u64, Status> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn string(&mut self) -> std::result::Result<String, Status> {
        let count = self.u32()? as usize;
        let value = std::str::from_utf8(self.take(count)?).map_err(|_| Status::InvalidArgument)?;
        if value.is_empty() || value.len() > 4096 || value.contains('\0') {
            return Err(Status::InvalidArgument);
        }
        Ok(value.to_owned())
    }
    fn finish(&self) -> std::result::Result<(), Status> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Status::InvalidArgument)
        }
    }
}
fn put32(out: &mut Vec<u8>, value: u32) {
    out.extend(value.to_le_bytes());
}
fn put64(out: &mut Vec<u8>, value: u64) {
    out.extend(value.to_le_bytes());
}
fn string(value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    put32(&mut out, value.len() as u32);
    out.extend(value.as_bytes());
    out
}
fn stats(s: EngineStats) -> Vec<u8> {
    let mut out = Vec::new();
    for value in [64, 1] {
        put32(&mut out, value);
    }
    for value in [s.active_work, s.grants, s.held_ns, s.waited_ns] {
        put64(&mut out, value);
    }
    for value in [s.waiting, s.holding, s.weight, s.tenancy] {
        put32(&mut out, value);
    }
    put64(&mut out, s.capabilities);
    out
}

fn frame(stream: &mut UnixStream) -> std::io::Result<(u32, Vec<u8>)> {
    let mut header = [0u8; 16];
    stream.read_exact(&mut header)?;
    let word = |offset| u32::from_le_bytes(header[offset..offset + 4].try_into().unwrap());
    let size = word(12) as usize;
    if word(0) != MAGIC || word(4) != 1 || size > LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid Corby frame",
        ));
    }
    let mut body = vec![0; size];
    stream.read_exact(&mut body)?;
    Ok((word(8), body))
}
fn send(stream: &mut UnixStream, opcode: u32, body: &[u8]) -> std::io::Result<()> {
    if body.len() > LIMIT {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let mut out = Vec::with_capacity(16 + body.len());
    for value in [MAGIC, 1, opcode, body.len() as u32] {
        put32(&mut out, value);
    }
    out.extend(body);
    stream.write_all(&out)
}
fn reply(stream: &mut UnixStream, opcode: u32, result: Reply) -> std::io::Result<()> {
    let mut body = Vec::new();
    match result {
        Ok(out) => {
            put32(&mut body, 0);
            body.extend(out);
        }
        Err(error) => put32(&mut body, error as u32),
    }
    send(stream, opcode, &body)
}

fn execute(inner: &Inner, session: &Arc<Session>, opcode: u32, bytes: &[u8]) -> Reply {
    if session.closed.load(Ordering::Acquire) || inner.stopping.load(Ordering::Acquire) {
        return Err(Status::Unavailable);
    }
    let mut d = Decoder::new(bytes);
    let mut out = Vec::new();
    match opcode {
        3 => {
            d.finish()?;
            let state = inner.coordinator.lock()?;
            if state.entries.len() > 4096 {
                return Err(Status::Busy);
            }
            put32(&mut out, state.entries.len() as u32);
            for id in state.entries.keys() {
                put64(&mut out, *id);
            }
        }
        4..=7 => {
            let id = d.u64()?;
            let weight = if opcode == 6 { Some(d.u32()?) } else { None };
            d.finish()?;
            let mut state = inner.coordinator.lock()?;
            let entry = state.entries.get_mut(&id).ok_or(Status::NotFound)?;
            match opcode {
                4 => out = stats(entry.stats()),
                5 => {
                    put32(&mut out, entry.devices.len() as u32);
                    for device in &entry.devices {
                        put32(&mut out, *device as u32);
                    }
                }
                6 => {
                    let weight = weight.unwrap();
                    if weight == 0 {
                        return Err(Status::InvalidArgument);
                    }
                    entry.weight = weight;
                    inner.coordinator.notify();
                }
                7 => out = string(&entry.label),
                _ => unreachable!(),
            }
        }
        8 => {
            let label = d.string()?;
            let count = d.u32()?;
            if count == 0 || count > 4096 {
                return Err(Status::InvalidArgument);
            }
            let mut devices = Vec::new();
            for _ in 0..count {
                devices.push(d.u32()? as i32);
            }
            let weight = d.u32()?;
            let tenancy = d.u32()?;
            let capabilities = d.u64()?;
            d.finish()?;
            if capabilities & !(crate::CAP_COOPERATIVE | crate::CAP_MEMORY_RECLAIM) != 0 {
                return Err(Status::Unsupported);
            }
            let engine =
                inner
                    .coordinator
                    .register(label, devices, weight, tenancy, capabilities)?;
            put64(&mut out, session.insert(Object::Engine(engine))?);
        }
        9..=19 => {
            let id = d.u64()?;
            let parameter = match opcode {
                11 => d.u32()? as u64,
                17 | 18 => d.u64()?,
                _ => 0,
            };
            d.finish()?;
            // Exclude the yield observer until the released object is removed.
            // Otherwise it could observe a closed lease between close/remove and
            // mistake the missing holder for a corrupt coordinator.
            if opcode == 19 {
                let mut objects = session.objects.lock().map_err(|_| Status::Internal)?;
                match objects.get(&id) {
                    Some(Object::Lease(l)) => l.close()?,
                    None => return Ok(out),
                    _ => return Err(Status::InvalidArgument),
                }
                let removed = objects.remove(&id);
                drop(objects);
                drop(removed);
                return Ok(out);
            }
            // Consuming commands are idempotent within this session/incarnation.
            let object = match session.get(id) {
                Ok(o) => o,
                Err(Status::NotFound) if matches!(opcode, 10 | 16 | 19) => return Ok(out),
                Err(e) => return Err(e),
            };
            match (opcode, object) {
                (9, Object::Engine(e)) => e.retire()?,
                (10, Object::Engine(e)) => {
                    e.unregister()?;
                    session.remove(id)?;
                }
                (11, Object::Engine(e)) => e.set_weight(parameter as u32)?,
                (12, Object::Engine(e)) => {
                    out = stats(e.coordinator.lock()?.entries[&e.id].stats())
                }
                (13, Object::Engine(e)) => {
                    out = string(&e.coordinator.lock()?.entries[&e.id].label)
                }
                (14, Object::Engine(e)) => {
                    put64(&mut out, session.insert(Object::Work(e.begin()?))?)
                }
                (15, Object::Work(w)) => w.cancel()?,
                (16, Object::Work(w)) => {
                    if !w.idle()? {
                        return Err(Status::Busy);
                    }
                    session.remove(id)?;
                }
                (17, Object::Work(w)) => put64(
                    &mut out,
                    session.insert(Object::Lease(Arc::new(w.acquire(parameter)?)))?,
                ),
                (18, Object::Lease(l)) => put32(&mut out, u32::from(l.should_yield(parameter)?)),
                (19, Object::Lease(l)) => {
                    l.close()?;
                    session.remove(id)?;
                }
                _ => return Err(Status::InvalidArgument),
            }
        }
        20..=27 => return extensions::execute(session, opcode, &mut d),
        _ => return Err(Status::Unsupported),
    }
    Ok(out)
}

/// Monitor the session for changes.
fn monitor(inner: &Inner, session: &Session, stream: &mut UnixStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_millis(1)))?;
    let mut published = BTreeMap::new();
    while !inner.stopping.load(Ordering::Acquire) && !session.closed.load(Ordering::Acquire) {
        let mut byte = [0];
        match stream.read(&mut byte) {
            Ok(_) => break, // EOF, or invalid traffic on the server-push channel.
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break,
        }
        let mut present = BTreeMap::new();
        let mut notifications = Vec::new();
        {
            // Borrow under the object lock: cloning a lease here would extend
            // its ownership past a successful release RPC, making an immediate
            // work_end spuriously BUSY (or retaining GPU permission longer).
            let objects = session
                .objects
                .lock()
                .map_err(|_| std::io::ErrorKind::Other)?;
            for (&id, object) in objects.iter() {
                let Object::Lease(lease) = object else {
                    continue;
                };
                if lease.acquired() != Ok(()) || lease.work.resource != crate::scheduler::COMPUTE {
                    continue;
                }
                let flags = if lease
                    .should_yield(u64::MAX)
                    .map_err(|_| std::io::ErrorKind::Other)?
                {
                    2
                } else if lease
                    .should_yield(0)
                    .map_err(|_| std::io::ErrorKind::Other)?
                {
                    1
                } else {
                    0
                };
                present.insert(id, flags);
                if published.get(&id) != Some(&flags) {
                    let mut out = Vec::new();
                    put64(&mut out, id);
                    put32(&mut out, flags);
                    notifications.push(out);
                }
            }
        }
        // Never hold scheduler/session locks while writing to a slow peer.
        for out in notifications {
            session.notify(100, out);
        }
        published = present;
    }
    Ok(())
}

/// Handle a new connection.
fn connection(inner: Arc<Inner>, mut stream: UnixStream) -> std::io::Result<()> {
    if !same_user(&stream) {
        return Err(std::io::ErrorKind::PermissionDenied.into());
    }
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let (opcode, body) = frame(&mut stream)?;
    if opcode == 0 {
        let mut d = Decoder::new(&body);
        let features = d.u64().map_err(|_| std::io::ErrorKind::InvalidData)?;
        if d.finish().is_err() || features & !FEATURES != 0 {
            return reply(&mut stream, 0, Err(Status::Unsupported));
        }
        if !inner.accepting.load(Ordering::Acquire) {
            return reply(&mut stream, 0, Err(Status::Unavailable));
        }
        let mut token = [0u8; 16];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut token)?;
        let (notifications, receiver) = sync_channel(8192);
        let session = Arc::new(Session {
            objects: Default::default(),
            next: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            notifications,
            control: stream.try_clone()?,
        });
        inner
            .sessions
            .sessions
            .lock()
            .map_err(|_| std::io::ErrorKind::Other)?
            .insert(token, session.clone());
        let mut out = Vec::new();
        put64(&mut out, FEATURES);
        out.extend(token);
        reply(&mut stream, 0, Ok(out))?;
        let mut push = stream.try_clone()?;
        let weak = Arc::downgrade(&session);
        let writer = std::thread::Builder::new()
            .name("corby-push".into())
            .spawn(move || {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while let Ok(Some((opcode, body))) = receiver.recv() {
                        if send(&mut push, opcode, &body).is_err() {
                            break;
                        }
                    }
                }));
                if let Some(session) = weak.upgrade() {
                    session.disconnect();
                }
            })?;
        let result = monitor(&inner, &session, &mut stream);
        session.disconnect();
        let _ = writer.join();
        // Keep quarantined objects pinned. Empty sessions can be removed only
        // after their last in-flight request has finished (Arc strong count).
        result
    } else {
        if body.len() < 16 {
            return reply(&mut stream, opcode, Err(Status::InvalidArgument));
        }
        let token: [u8; 16] = body[..16].try_into().unwrap();
        let session = inner
            .sessions
            .sessions
            .lock()
            .map_err(|_| std::io::ErrorKind::Other)?
            .get(&token)
            .cloned();
        let result = match session {
            Some(session) => std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute(&inner, &session, opcode, &body[16..])
            }))
            .unwrap_or_else(|_| {
                let _ = inner.coordinator.retire();
                inner.coordinator.cancel_all();
                Err(Status::Internal)
            }),
            None => Err(Status::Unavailable),
        };
        reply(&mut stream, opcode, result)
    }
}

/// Serve the IPC server.
pub(crate) fn serve(inner: Arc<Inner>, listener: UnixListener) -> std::result::Result<(), Status> {
    std::thread::Builder::new()
        .name("corby-listener".into())
        .spawn(move || {
            let mut sweep = 0_u32;
            while !inner.stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if inner.stopping.load(Ordering::Acquire) {
                            break;
                        }
                        if inner.sessions.workers.fetch_add(1, Ordering::AcqRel) >= 128 {
                            inner.sessions.workers.fetch_sub(1, Ordering::AcqRel);
                            continue;
                        }
                        let worker = inner.clone();
                        if std::thread::Builder::new()
                            .name("corby-session".into())
                            .spawn(move || {
                                let inner = worker;
                                let _ =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        connection(inner.clone(), stream)
                                    }));
                                inner.sessions.workers.fetch_sub(1, Ordering::AcqRel);
                            })
                            .is_err()
                        {
                            inner.sessions.workers.fetch_sub(1, Ordering::AcqRel);
                            inner.accepting.store(false, Ordering::Release);
                            let _ = inner.coordinator.retire();
                            inner.coordinator.cancel_all();
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        let _ = inner.coordinator.retire();
                        inner.coordinator.cancel_all();
                        break;
                    }
                }
                sweep = sweep.wrapping_add(1);
                if sweep % 1000 == 0 {
                    if let Ok(mut sessions) = inner.sessions.sessions.lock() {
                        sessions.retain(|_, session| {
                            !session.closed.load(Ordering::Acquire)
                                || Arc::strong_count(session) > 1
                                || session
                                    .objects
                                    .lock()
                                    .map_or(true, |objects| !objects.is_empty())
                        });
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(|_| Status::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoder_rejects_short_strings_and_trailing_command_fields() {
        let mut bytes = Vec::new();
        put32(&mut bytes, 4000);
        bytes.extend(b"x");
        assert_eq!(Decoder::new(&bytes).string(), Err(Status::InvalidArgument));
        assert_eq!(Decoder::new(&[1]).u64(), Err(Status::InvalidArgument));
        let mut decoder = Decoder::new(&[0; 12]);
        decoder.u64().unwrap();
        assert_eq!(decoder.finish(), Err(Status::InvalidArgument));
    }

    #[test]
    fn frame_bounds_and_wire_version_are_checked_before_allocation() {
        for (magic, version, size) in [(MAGIC, 1, 65537), (MAGIC, 2, 0), (0, 1, 0)] {
            let (mut reader, mut writer) = UnixStream::pair().unwrap();
            let mut header = Vec::new();
            for word in [magic, version, 3, size] {
                put32(&mut header, word);
            }
            writer.write_all(&header).unwrap();
            assert_eq!(
                frame(&mut reader).unwrap_err().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        let mut header = Vec::new();
        for word in [MAGIC, 1, 3, 16] {
            put32(&mut header, word);
        }
        writer.write_all(&header).unwrap();
        drop(writer);
        assert_eq!(
            frame(&mut reader).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }
}
