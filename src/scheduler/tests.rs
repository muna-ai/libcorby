#[cfg(test)]
mod fairness_tests {
    use super::super::*;

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
            s.entries.get_mut(&a.id).unwrap().lanes[COMPUTE].pending = Some(Pending {
                work: wa.id,
                ticket: 1,
                lease: 1,
            });
            s.entries.get_mut(&b.id).unwrap().lanes[COMPUTE].pending = Some(Pending {
                work: wb.id,
                ticket: 2,
                lease: 2,
            });
            let mut grants = [0; 2];
            for _ in 0..400 {
                let id = s.winner(COMPUTE).unwrap();
                grants[usize::from(id == b.id)] += 1;
                let e = s.entries.get_mut(&id).unwrap();
                e.lanes[COMPUTE].pass += 3_000_000 * SCALE / e.weight as u128;
            }
            assert_eq!(grants, [100, 300]);
            s.entries.get_mut(&a.id).unwrap().lanes[COMPUTE].pending = None;
            s.entries.get_mut(&b.id).unwrap().lanes[COMPUTE].pending = None;
            s.floors.insert((COMPUTE, 0), 999_999_999_999);
        }
        let held = wa.acquire(0).unwrap();
        assert!(c.lock().unwrap().entries[&a.id].lanes[COMPUTE].pass >= 999_999_999_999);
        drop(held);
        assert!(c.lock().unwrap().entries[&a.id].held_ns > 0);
    }

    #[test]
    fn poisoned_state_returns_error_instead_of_free_running() {
        let c = Arc::new(Coordinator::default());
        let other = c.clone();
        let _ = std::thread::spawn(move || {
            let _state = other.shared.state.lock().unwrap();
            panic!("poison");
        })
        .join();
        assert!(matches!(c.lock(), Err(Status::Internal)));
    }
}
