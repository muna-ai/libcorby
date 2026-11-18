/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

use crate::scheduler::{
    Coordinator, Engine, Requirement, COMPUTE, INITIALIZATION, MEMORY, OPPORTUNISTIC,
};
use crate::Status;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

const SECOND: Duration = Duration::from_secs(2);
fn engine(c: &Arc<Coordinator>, name: &str, devices: &[i32]) -> Arc<Engine> {
    c.register(name.into(), devices.to_vec(), 1, 0, 3).unwrap()
}
fn receive<T>(rx: &mpsc::Receiver<T>) -> T {
    rx.recv_timeout(SECOND).expect("event not delivered")
}

#[test]
fn async_acquisition_wakes_once_and_late_registration_is_supported() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let wa = a.begin().unwrap();
    let wb = b.begin().unwrap();
    let held = wa.acquire(0).unwrap();
    let pending = wb.acquire_ex(u64::MAX).unwrap();
    assert_eq!(pending.acquired(), Err(Status::Pending));
    assert_eq!(pending.should_yield(0), Err(Status::Pending));
    let (tx, rx) = mpsc::channel();
    pending
        .register_handler(Arc::new(move |s| tx.send(s).unwrap()))
        .unwrap();
    drop(held);
    assert_eq!(receive(&rx), Status::Ok);
    assert_eq!(pending.acquired(), Ok(()));
    assert_eq!(
        pending.register_handler(Arc::new(|_| {})),
        Err(Status::Busy)
    );
    drop(pending);
    assert!(rx.try_recv().is_err());
    // Receiving a callback's signal does not mean delivery has returned.
    // Drop defers cleanup in that window; wait for the actual release before
    // exercising registration on an already-completed acquisition.
    let completed = wa.acquire_ex(u64::MAX).unwrap();
    completed.wait().unwrap();
    let (tx, rx) = mpsc::channel();
    completed
        .register_handler(Arc::new(move |s| tx.send(s).unwrap()))
        .unwrap();
    assert_eq!(receive(&rx), Status::Ok);
}

#[test]
fn async_deadline_and_cancel_report_terminal_errors_without_revoking_holders() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let wa = a.begin().unwrap();
    let wb = b.begin().unwrap();
    let held = wa.acquire(0).unwrap();
    let timed = wb.acquire_ex(2_000_000).unwrap();
    let (tx, rx) = mpsc::channel();
    timed
        .register_handler(Arc::new(move |s| tx.send(s).unwrap()))
        .unwrap();
    assert_eq!(receive(&rx), Status::Timeout);
    assert_eq!(timed.acquired(), Err(Status::Timeout));
    drop(timed);
    let pending = wb.acquire_ex(u64::MAX).unwrap();
    let (tx, rx) = mpsc::channel();
    pending
        .register_handler(Arc::new(move |s| tx.send(s).unwrap()))
        .unwrap();
    wb.cancel().unwrap();
    assert_eq!(receive(&rx), Status::Cancelled);
    wa.cancel().unwrap();
    assert_eq!(held.acquired(), Ok(()));
    assert!(held.should_yield(0).unwrap());
}

#[test]
fn releasing_pending_lease_suppresses_queued_callback_and_frees_waiter_slot() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let wa = a.begin().unwrap();
    let wb = b.begin().unwrap();
    let held = wa.acquire(0).unwrap();
    let pending = wb.acquire_ex(u64::MAX).unwrap();
    let (tx, rx) = mpsc::channel();
    pending
        .register_handler(Arc::new(move |s| tx.send(s).unwrap()))
        .unwrap();
    pending.close().unwrap();
    drop(pending);
    drop(held);
    let _granted = wb.acquire(0).unwrap();
    assert!(rx.try_recv().is_err());
}

#[test]
fn memory_lease_does_not_stop_compute_and_reports_partial_reclamation_immediately() {
    let c = Arc::new(Coordinator::default());
    let newcomer = engine(&c, "new", &[0, 1]);
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[1]);
    let unrelated = engine(&c, "unrelated", &[2]);
    unrelated
        .set_memory_handler(Some(Arc::new(|_| panic!("wrong device"))))
        .unwrap();
    let (ta, ra) = mpsc::channel();
    let (tb, rb) = mpsc::channel();
    a.set_memory_handler(Some(Arc::new(move |r| ta.send(r).unwrap())))
        .unwrap();
    b.set_memory_handler(Some(Arc::new(move |r| tb.send(r).unwrap())))
        .unwrap();
    let compute = a.begin().unwrap();
    let _executing = compute.acquire(0).unwrap();
    let init = newcomer.begin_ex(MEMORY, INITIALIZATION).unwrap();
    let lease = init.acquire(0).unwrap();
    let (tx, rx) = mpsc::channel();
    lease
        .reclaim_memory(
            vec![Requirement::new(0, 100), Requirement::new(1, 200)],
            u64::MAX,
            Arc::new(move |p| tx.send(p).unwrap()),
        )
        .unwrap();
    let first = receive(&ra);
    let second = receive(&rb);
    assert_eq!(
        first
            .requirements()
            .iter()
            .map(|r| (r.device_id, r.bytes))
            .collect::<Vec<_>>(),
        vec![(0, 100)]
    );
    assert_eq!(second.requirements()[0].device_id, 1);
    assert!(!a.idle().unwrap());
    first.complete(Status::Ok, 120).unwrap();
    let partial = loop {
        let p = receive(&rx);
        if p.completed == 1 {
            break p;
        }
    };
    assert_eq!(
        (partial.pending, partial.status, partial.released_bytes),
        (1, Status::Pending as i32, 120)
    );
    second.complete(Status::Ok, 0).unwrap();
    let final_event = loop {
        let p = receive(&rx);
        if p.status != Status::Pending as i32 {
            break p;
        }
    };
    assert_eq!(
        (
            final_event.completed,
            final_event.pending,
            final_event.failed
        ),
        (2, 0, 0)
    );
    assert_eq!(lease.should_yield(0), Err(Status::Unsupported));
}

#[test]
fn late_reclaim_ack_remains_valid_after_requester_release() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let (tx, rx) = mpsc::channel();
    b.set_memory_handler(Some(Arc::new(move |r| tx.send(r).unwrap())))
        .unwrap();
    let work = a.begin_ex(MEMORY, 0).unwrap();
    let lease = work.acquire(0).unwrap();
    lease
        .reclaim_memory(vec![Requirement::new(0, 1)], u64::MAX, Arc::new(|_| {}))
        .unwrap();
    let request = receive(&rx);
    // close may race the progress callback; destruction safely defers fencing.
    drop(lease);
    assert!(!b.idle().unwrap());
    assert_eq!(b.unregister(), Err(Status::Busy));
    request.complete(Status::Ok, 1).unwrap();
    assert_eq!(
        request.complete(Status::Ok, 1),
        Err(Status::InvalidArgument)
    );
    drop(request);
    assert!(b.idle().unwrap());
    b.unregister().unwrap();
    assert!(matches!(b.begin(), Err(Status::Retired)));
    let lease = work.acquire(u64::MAX).unwrap();
    let (tx, rx) = mpsc::channel();
    lease
        .reclaim_memory(
            vec![Requirement::new(0, 1)],
            u64::MAX,
            Arc::new(move |p| tx.send(p).unwrap()),
        )
        .unwrap();
    let final_event = receive(&rx);
    assert_eq!(
        (final_event.status, final_event.completed),
        (Status::Ok as i32, 0)
    );
}

#[test]
fn missing_handler_and_deadline_are_terminal_not_false_successes() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let work = a.begin_ex(MEMORY, 0).unwrap();
    let lease = work.acquire(0).unwrap();
    let (tx, rx) = mpsc::channel();
    lease
        .reclaim_memory(
            vec![Requirement::new(0, 1)],
            u64::MAX,
            Arc::new(move |p| tx.send(p).unwrap()),
        )
        .unwrap();
    let p = loop {
        let p = receive(&rx);
        if p.status != Status::Pending as i32 {
            break p;
        }
    };
    assert_eq!((p.status, p.failed), (Status::Unsupported as i32, 1));
    drop(lease);
    let (tickets, pending) = mpsc::channel();
    b.set_memory_handler(Some(Arc::new(move |r| tickets.send(r).unwrap())))
        .unwrap();
    let lease = work.acquire(u64::MAX).unwrap();
    let (tx, rx) = mpsc::channel();
    lease
        .reclaim_memory(
            vec![Requirement::new(0, 1)],
            2_000_000,
            Arc::new(move |p| tx.send(p).unwrap()),
        )
        .unwrap();
    let ticket = receive(&pending);
    let p = loop {
        let p = receive(&rx);
        if p.status != Status::Pending as i32 {
            break p;
        }
    };
    assert_eq!((p.status, p.pending), (Status::Timeout as i32, 1));
    ticket.complete(Status::Ok, 0).unwrap();
}

#[test]
fn memory_groups_are_atomic_and_initializers_only_suppress_opportunistic_growth() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let pair = engine(&c, "pair", &[0, 1]);
    let b = engine(&c, "b", &[1]);
    let opportunist = engine(&c, "cache", &[0]);
    let init = a.begin_ex(MEMORY, INITIALIZATION).unwrap();
    let cache = opportunist.begin_ex(MEMORY, OPPORTUNISTIC).unwrap();
    let waiting = cache.acquire_ex(u64::MAX).unwrap();
    assert_eq!(waiting.acquired(), Err(Status::Pending));
    let other_init = pair.begin_ex(MEMORY, INITIALIZATION).unwrap();
    let held = other_init.acquire(0).unwrap();
    let required = b.begin_ex(MEMORY, 0).unwrap();
    assert!(matches!(required.acquire(0), Err(Status::Timeout)));
    let compute = b.begin_ex(COMPUTE, 0).unwrap();
    let _compute_held = compute.acquire(0).unwrap();
    drop(held);
    let _required = required.acquire(0).unwrap();
    drop(other_init);
    assert_eq!(waiting.acquired(), Err(Status::Pending));
    drop(init);
    waiting.wait().unwrap();
}

#[test]
fn dropping_rust_lease_inside_its_completion_callback_releases_after_delivery() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let wa = a.begin().unwrap();
    let wb = b.begin().unwrap();
    let held = wa.acquire(0).unwrap();
    let lease = wb.acquire_ex(u64::MAX).unwrap();
    let owner = Arc::new(Mutex::new(Some(lease)));
    let callback_owner = owner.clone();
    let (tx, rx) = mpsc::channel();
    owner
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .register_handler(Arc::new(move |_| {
            drop(callback_owner.lock().unwrap().take());
            tx.send(()).unwrap();
        }))
        .unwrap();
    drop(held);
    receive(&rx);
    // A sync acquisition waits on the actual release event, not a polling delay.
    let _again = wa.acquire(u64::MAX).unwrap();
}

#[test]
fn poisoned_coordinator_resolves_pending_promises_without_granting_permission() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, "a", &[0]);
    let b = engine(&c, "b", &[0]);
    let wa = a.begin().unwrap();
    let wb = b.begin().unwrap();
    let held = wa.acquire(0).unwrap();
    let pending = wb.acquire_ex(u64::MAX).unwrap();
    let (tx, rx) = mpsc::channel();
    pending
        .register_handler(Arc::new(move |s| tx.send(s).unwrap()))
        .unwrap();
    let other = c.clone();
    let _ = std::thread::spawn(move || {
        let _state = other.lock().unwrap();
        panic!("synthetic coordinator failure");
    })
    .join();
    c.notify();
    assert_eq!(receive(&rx), Status::Internal);
    assert_eq!(pending.acquired(), Err(Status::Internal));
    assert_eq!(held.should_yield(0), Err(Status::Internal));
}
