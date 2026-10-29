use std::{array, rc::Rc};

use dope_core::driver::schedule::clock::timer::{heap, heap::State};

#[test]
fn keeps_lease_identity() {
    let store = (heap::raw::Store::<u128, u64>::try_new(3))
        .expect("the three-slot timer store must allocate");
    // SAFETY: this test owns `store` exclusively, so no other loan overlaps.
    unsafe {
        store.loan(|mut heap| {
            let first = heap
                .vacancy()
                .expect("a fresh three-slot store must have a first vacancy")
                .lease(10);
            let second = heap
                .vacancy()
                .expect("a three-slot store with one resident must have a second vacancy")
                .lease(20);
            let third = heap
                .vacancy()
                .expect("a three-slot store with two residents must have a third vacancy")
                .lease(30);
            assert!(heap.vacancy().is_none());

            heap.set(&third, 7);
            heap.set(&second, 7);
            heap.set(&first, 9);
            assert_eq!(heap.peek().copied(), Some(7));
            assert_eq!(heap.priority(&first), Some(&9));
            heap.set(&first, 3);
            {
                let min = heap.minimum().expect("armed minimum");
                assert_eq!(*min.priority(), 3);
                let fired = min.fire();
                assert_eq!(*fired.resident(), 10);
            }
            assert!((heap.state(&first)) == (State::Fired));
            heap.clear(&first);
            assert!((heap.state(&first)) == (State::Idle));
            assert_eq!(heap.priority(&second), Some(&7));
            heap.clear(&second);
            assert_eq!(heap.release(first), 10);

            let reused = heap
                .vacancy()
                .expect("a released resident slot must be vacant")
                .lease(41);
            assert_eq!(*heap.resident(&reused), 41);
        });
    }
}

#[test]
fn drops_values_once() {
    let item0 = Rc::new(());
    let item1 = Rc::new(());
    let prio0 = Rc::new(());
    let prio1 = Rc::new(());
    {
        let store = (heap::raw::Store::<Rc<()>, Rc<()>>::try_new(2))
            .expect("the two reference-counted slots must allocate");
        // SAFETY: this test owns `store` exclusively, so no other loan overlaps.
        unsafe {
            store.loan(|mut heap| {
                let first = heap
                    .vacancy()
                    .expect("a fresh two-slot store must have a first vacancy")
                    .lease(item0.clone());
                let second = heap
                    .vacancy()
                    .expect("a two-slot store with one resident must have a second vacancy")
                    .lease(item1.clone());
                heap.set(&first, prio0.clone());
                heap.set(&second, prio1.clone());
                {
                    let fired = heap
                        .minimum()
                        .expect("the first lease must be minimum")
                        .fire();
                    assert!(Rc::ptr_eq(fired.resident(), &item0));
                    assert_eq!(Rc::strong_count(&prio0), 1);
                }
                assert_eq!(heap.priority(&second), Some(&prio1));
                heap.clear(&second);
                assert_eq!(Rc::strong_count(&prio1), 1);
                heap.set(&second, prio1.clone());
                let released = heap.release(first);
                assert!(Rc::ptr_eq(&released, &item0));
            });
        }
    }
    assert_eq!(Rc::strong_count(&item0), 1);
    assert_eq!(Rc::strong_count(&item1), 1);
    assert_eq!(Rc::strong_count(&prio0), 1);
    assert_eq!(Rc::strong_count(&prio1), 1);
}

#[test]
fn matches_schedule_model() {
    const CAPACITY: usize = 32;
    let store = (heap::raw::Store::<u64, u64>::try_new(CAPACITY))
        .expect("the model timer slots must allocate");
    // SAFETY: this test owns `store` exclusively, so no other loan overlaps.
    unsafe {
        store.loan(|mut heap| {
            let slots = array::from_fn::<_, CAPACITY, _>(|index| {
                heap.vacancy()
                    .expect("one vacancy must remain for each model resident")
                    .lease(index as u64)
            });
            let mut prios = [None; CAPACITY];
            let mut fired = [false; CAPACITY];
            let mut random = 0x9e37_79b9_7f4a_7c15_u64;
            for step in 0..16_384 {
                random ^= random << 7;
                random ^= random >> 9;
                let index = random as usize & (CAPACITY - 1);
                match random >> 61 {
                    0..=3 => {
                        let prio = random.rotate_left(23) & 127;
                        assert_eq!(heap.priority(&slots[index]).copied(), prios[index]);
                        heap.set(&slots[index], prio);
                        prios[index] = Some(prio);
                        fired[index] = false;
                    }
                    4..=5 => {
                        assert_eq!(heap.priority(&slots[index]).copied(), prios[index]);
                        heap.clear(&slots[index]);
                        prios[index] = None;
                        fired[index] = false;
                    }
                    _ => {
                        let want = prios
                            .iter()
                            .enumerate()
                            .filter_map(|(index, prio)| prio.map(|value| (index, value)))
                            .min_by_key(|&(index, prio)| (prio, index));
                        if let Some((index, prio)) = want {
                            let min = heap.minimum().expect("model minimum");
                            assert_eq!(*min.priority(), prio);
                            assert_eq!(*min.fire().resident(), index as u64);
                            prios[index] = None;
                            fired[index] = true;
                        }
                    }
                }
                let want = prios
                    .iter()
                    .enumerate()
                    .filter_map(|(index, prio)| prio.map(|value| (index, value)))
                    .min_by_key(|&(index, prio)| (prio, index));
                assert_eq!(heap.peek().copied(), want.map(|(_, prio)| prio));
                if step & 255 == 0 {
                    for index in 0..CAPACITY {
                        let state = if prios[index].is_some() {
                            State::Armed
                        } else if fired[index] {
                            State::Fired
                        } else {
                            State::Idle
                        };
                        assert!((heap.state(&slots[index])) == (state));
                        assert_eq!(*heap.resident(&slots[index]), index as u64);
                    }
                }
            }
            for (index, slot) in slots.into_iter().enumerate() {
                assert_eq!(heap.release(slot), index as u64);
            }
        });
    }
}
