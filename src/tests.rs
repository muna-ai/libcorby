use super::*;
use crate::scheduler::Engine;
use std::sync::{mpsc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
static RUNTIME_TEST: Mutex<()> = Mutex::new(());

fn engine(c: &Arc<Coordinator>, devices: &[i32], dedicated: bool) -> Arc<Engine> {
    c.register(
        "test".into(),
        devices.into(),
        1,
        u32::from(dedicated),
        CAP_COOPERATIVE,
    )
    .unwrap()
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(Instant::now() < deadline, "condition did not become true");
        thread::yield_now();
    }
}

#[test]
fn shared_exclusion_disjoint_progress_and_zero_timeout() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, &[0], false).begin().unwrap();
    let b = engine(&c, &[0], false).begin().unwrap();
    let d = engine(&c, &[1], false).begin().unwrap();
    let held = a.acquire(0).unwrap();
    assert!(matches!(b.acquire(0), Err(Status::Timeout)));
    assert!(b.idle().unwrap());
    let disjoint = d.acquire(0).unwrap();
    drop(held);
    assert!(b.acquire(0).is_ok());
    drop(disjoint);
}

#[test]
fn atomic_group_wait_never_holds_a_partial_group() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, &[0], false).begin().unwrap();
    let pair = engine(&c, &[0, 1], false).begin().unwrap();
    let b = engine(&c, &[1], true);
    let held = a.acquire(0).unwrap();
    let pair_clone = pair.clone();
    let thread = thread::spawn(move || pair_clone.acquire(2_000_000_000).unwrap());
    wait_until(|| c.lock().unwrap().entries[&pair.engine.id].stats().waiting == 1);
    let demand = b.begin().unwrap();
    let other = demand.acquire(0).unwrap();
    drop(held);
    assert_eq!(
        c.lock().unwrap().entries[&pair.engine.id].stats().holding,
        0
    );
    drop(other);
    drop(demand);
    drop(thread.join().unwrap());
}

#[test]
fn an_older_group_cannot_be_leapfrogged_by_single_device_arrivals() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, &[0], false).begin().unwrap();
    let pair = engine(&c, &[0, 1], false).begin().unwrap();
    let b = engine(&c, &[1], false).begin().unwrap();
    let held = a.acquire(0).unwrap();
    let pair_clone = pair.clone();
    let thread = thread::spawn(move || pair_clone.acquire(2_000_000_000).unwrap());
    wait_until(|| c.lock().unwrap().entries[&pair.engine.id].stats().waiting == 1);
    assert!(matches!(b.acquire(0), Err(Status::Timeout)));
    assert!(held.should_yield(0).unwrap());
    drop(held);
    drop(thread.join().unwrap());
    assert!(b.acquire(0).is_ok());
}

#[test]
fn dedicated_demand_covers_gaps_and_overrides_minimum_hold() {
    let c = Arc::new(Coordinator::default());
    let shared = engine(&c, &[0, 1], false).begin().unwrap();
    let owner = engine(&c, &[0], true);
    let held = shared.acquire(0).unwrap();
    let demand1 = owner.begin().unwrap();
    let demand2 = owner.begin().unwrap();
    assert!(held.should_yield(u64::MAX).unwrap());
    drop(held);
    assert!(matches!(shared.acquire(0), Err(Status::Timeout)));
    drop(demand1.acquire(0).unwrap());
    assert!(matches!(shared.acquire(0), Err(Status::Timeout)));
    drop(demand1);
    assert!(matches!(shared.acquire(0), Err(Status::Timeout)));
    // Cancelling demand doesn't permit a peer to race unfinished GPU work.
    demand2.cancel().unwrap();
    assert!(matches!(shared.acquire(0), Err(Status::Timeout)));
    drop(demand2);
    assert!(shared.acquire(0).is_ok());
}

#[test]
fn reservation_and_capability_admission_fail_closed() {
    let c = Arc::new(Coordinator::default());
    let owner = engine(&c, &[0, 1], true);
    assert!(matches!(
        c.register("owner2".into(), vec![1, 2], 1, 1, 1),
        Err(Status::Conflict)
    ));
    assert!(matches!(
        c.register("old".into(), vec![1], 1, 0, 0),
        Err(Status::Unsupported)
    ));
    assert!(c.register("disjoint".into(), vec![2], 1, 0, 0).is_ok());
    drop(owner);
    let old = c.register("old".into(), vec![1], 1, 0, 0).unwrap();
    assert!(matches!(
        c.register("new".into(), vec![1], 1, 1, 1),
        Err(Status::Unsupported)
    ));
    drop(old);
    assert!(c.register("new".into(), vec![1], 1, 1, 1).is_ok());
}

#[test]
fn cancellation_wakes_waiter_and_cleans_queue() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, &[0], false).begin().unwrap();
    let b = engine(&c, &[0], false).begin().unwrap();
    let held = a.acquire(0).unwrap();
    let waiter = b.clone();
    let (tx, rx) = mpsc::channel();
    let thread = thread::spawn(move || {
        tx.send(matches!(waiter.acquire(u64::MAX), Err(Status::Cancelled)))
            .unwrap()
    });
    wait_until(|| c.lock().unwrap().entries[&b.engine.id].stats().waiting == 1);
    b.cancel().unwrap();
    assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    thread.join().unwrap();
    assert!(b.idle().unwrap());
    assert!(!held.should_yield(0).unwrap());
    assert!(matches!(b.acquire(0), Err(Status::Cancelled)));
}

#[test]
fn timeout_cleans_queue_and_cancelled_holder_keeps_exclusivity() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, &[0], false).begin().unwrap();
    let b = engine(&c, &[0], false).begin().unwrap();
    let held = a.acquire(0).unwrap();
    assert!(matches!(b.acquire(1_000_000), Err(Status::Timeout)));
    assert!(b.idle().unwrap());
    a.cancel().unwrap();
    assert!(held.should_yield(u64::MAX).unwrap());
    assert!(matches!(b.acquire(0), Err(Status::Timeout)));
    drop(held);
    assert!(b.acquire(0).is_ok());
}

#[test]
fn retirement_rejects_new_work_and_preserves_existing_work() {
    let c = Arc::new(Coordinator::default());
    let e = engine(&c, &[0], true);
    let work = e.begin().unwrap();
    e.retire().unwrap();
    assert!(matches!(e.begin(), Err(Status::Retired)));
    assert!(!e.idle().unwrap());
    let held = work.acquire(0).unwrap();
    drop(e);
    assert_eq!(c.lock().unwrap().entries.len(), 1);
    drop(work);
    assert_eq!(c.lock().unwrap().entries.len(), 1);
    drop(held);
    assert!(c.lock().unwrap().entries.is_empty());
}

#[test]
fn fairness_requests_yield_for_a_queued_peer() {
    let c = Arc::new(Coordinator::default());
    let a = engine(&c, &[0], false).begin().unwrap();
    let b = engine(&c, &[0], false).begin().unwrap();
    let held = a.acquire(0).unwrap();
    let thread = thread::spawn(move || b.acquire(2_000_000_000).unwrap());
    wait_until(|| {
        c.lock()
            .unwrap()
            .entries
            .values()
            .any(|e| e.stats().waiting == 1)
    });
    assert!(!held.should_yield(u64::MAX).unwrap());
    assert!(held.should_yield(0).unwrap());
    drop(held);
    drop(thread.join().unwrap());
}

#[test]
fn overlapping_groups_stress_exclusivity() {
    let c = Arc::new(Coordinator::default());
    let barrier = Arc::new(Barrier::new(8));
    let occupancy = Arc::new(Mutex::new([false; 3]));
    let mut threads = Vec::new();
    for index in 0..8 {
        let devices = match index % 3 {
            0 => vec![0, 1],
            1 => vec![1, 2],
            _ => vec![2],
        };
        let work = engine(&c, &devices, false).begin().unwrap();
        let barrier = barrier.clone();
        let occupancy = occupancy.clone();
        threads.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..150 {
                let held = work.acquire(3_000_000_000).unwrap();
                {
                    let mut active = occupancy.lock().unwrap();
                    for &d in &devices {
                        assert!(!active[d as usize]);
                        active[d as usize] = true;
                    }
                }
                thread::yield_now();
                {
                    let mut active = occupancy.lock().unwrap();
                    for &d in &devices {
                        active[d as usize] = false;
                    }
                }
                drop(held);
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(c.lock().unwrap().entries.is_empty());
}

#[test]
fn invalid_registration_and_duplicate_acquire_are_rejected() {
    let c = Arc::new(Coordinator::default());
    for devices in [vec![], vec![-1], vec![0, 0]] {
        assert!(matches!(
            c.register("bad".into(), devices, 1, 0, 0),
            Err(Status::InvalidArgument)
        ));
    }
    let e = engine(&c, &[0], false);
    assert_eq!(e.set_weight(0), Err(Status::InvalidArgument));
    e.set_weight(3).unwrap();
    let work = e.begin().unwrap();
    let held = work.acquire(0).unwrap();
    assert!(matches!(work.acquire(0), Err(Status::Busy)));
    drop(held);
    assert_eq!(c.lock().unwrap().entries[&e.id].stats().grants, 1);
}

#[test]
fn abi_prefixes_unknown_features_and_panic_containment() {
    let _serial = RUNTIME_TEST.lock().unwrap();
    unsafe {
        let mut context = ptr::null_mut();
        let prefix = [8_u32, ABI_VERSION];
        assert_eq!(
            cor_context_create(prefix.as_ptr().cast(), &mut context),
            Status::Unavailable as i32
        );
        assert_eq!(cor_context_destroy(context), 0);
        let unknown = ContextOptions {
            size: 16,
            abi_version: ABI_VERSION,
            required_features: 1 << 63,
        };
        assert_eq!(
            cor_context_create(&unknown, &mut context),
            Status::Unsupported as i32
        );
        assert!(context.is_null());
        let short = [4_u32];
        assert_eq!(
            cor_context_create(short.as_ptr().cast(), &mut context),
            Status::InvalidArgument as i32
        );
        let newer = [8_u32, ABI_VERSION + 1];
        assert_eq!(
            cor_context_create(newer.as_ptr().cast(), &mut context),
            Status::Unsupported as i32
        );
        assert_eq!(cor_work_cancel(ptr::null()), Status::InvalidArgument as i32);
        assert_eq!(boundary(|| panic!("contained")), Status::Internal as i32);
    }
}

#[test]
fn explicit_runtime_lifecycle_and_scope_validation() {
    let _serial = RUNTIME_TEST.lock().unwrap();
    assert!(matches!(
        Runtime::start(RuntimeOptions {
            endpoint: Some("/tmp/unused".into()),
            ..Default::default()
        }),
        Err(Error(Status::InvalidArgument))
    ));
    let mut runtime = Runtime::start(RuntimeOptions::default()).unwrap();
    assert!(matches!(
        Runtime::start(RuntimeOptions::default()),
        Err(Error(Status::Conflict))
    ));
    let context = runtime.supervisor();
    let engine = context
        .register("owned", &[9], 1, Tenancy::Dedicated, true)
        .unwrap();
    let work = engine.begin().unwrap();
    let held = work.acquire(Some(Duration::ZERO)).unwrap();
    assert_eq!(runtime.shutdown(), Err(Error(Status::Busy)));
    assert!(matches!(engine.begin(), Err(Error(Status::Retired))));
    let mut raw = ptr::null_mut();
    assert_eq!(
        unsafe { cor_context_create(ptr::null(), &mut raw) },
        Status::Unavailable as i32
    );
    assert!(!held.should_yield(Duration::ZERO).unwrap());
    drop(held);
    drop(work);
    drop(engine);
    runtime.shutdown().unwrap();
    let mut next = Runtime::start(RuntimeOptions::default()).unwrap();
    // A second shutdown/Drop of the old owner must not withdraw its successor.
    runtime.shutdown().unwrap();
    drop(runtime);
    let context = next.context();
    let engine = context
        .register("next", &[9], 1, Tenancy::Shared, true)
        .unwrap();
    assert_eq!(context.engine_ids().unwrap(), vec![engine.id()]);
    context.set_engine_weight(engine.id(), 3).unwrap();
    assert_eq!(context.engine_stats(engine.id()).unwrap().weight, 3);
    assert_eq!(context.engine_devices(engine.id()).unwrap(), vec![9]);
    assert_eq!(context.engine_label(engine.id()).unwrap(), "next");
    drop(engine);
    next.shutdown().unwrap();

    let endpoint =
        std::env::temp_dir().join(format!("corby-rust-test-{}.sock", std::process::id()));
    let mut system = Runtime::start(RuntimeOptions {
        scope: RuntimeScope::System,
        endpoint: Some(endpoint.clone()),
    })
    .unwrap();
    assert_eq!(system.endpoint(), Some(endpoint.as_path()));
    assert!(endpoint.exists());
    system.shutdown().unwrap();
    assert!(!endpoint.exists());
}
