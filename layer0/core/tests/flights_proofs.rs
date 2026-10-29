use std::{array, mem};

use dope_core::driver::retained::flight::storage;
use dope_test::alloc::Tracker;
use o3::depot::arena::slab::Vacancy;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn takes_distinct_vacancies() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepare = Tracker::begin();
    let prepared = (builder.try_batch([2])).expect("two slots must prepare");
    let activity = prepare.finish();
    assert_eq!(activity.allocations, 2);
    assert_eq!(activity.allocated_bytes, 14 * mem::size_of::<usize>());
    let [slots] = prepared.groups();
    let tracker = Tracker::begin();
    let first = (slots.try_vacancy()).expect("first vacancy");
    let second = (slots.try_vacancy()).expect("second vacancy");
    assert!(slots.try_vacancy().is_err());
    let echo = {
        let second = second.reserve(11);
        second.echo()
    };
    {
        let first = first.reserve(7);
        assert!(first.echo() != echo);
        assert!(
            slots.try_vacancy().is_ok(),
            "recycling another entry preserves the held vacancy"
        );
    }
    let first = (slots.try_vacancy()).expect("first restored vacancy");
    let second = (slots.try_vacancy()).expect("second restored vacancy");
    assert!(slots.try_vacancy().is_err());
    drop((second, first));
    assert_eq!(tracker.finish().allocations, 0);
}

#[test]
fn keeps_rollback_bound() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1])).expect("one slot must prepare");
    let [slots] = prepared.groups();
    let tracker = Tracker::begin();
    for terminal in [false, true] {
        let echo = {
            let (echo, bound) = {
                let vacant = (slots.try_vacancy()).expect("rollback must restore capacity");
                let reserved = vacant.reserve(23);
                let echo = reserved.echo();
                let bound = (builder.drain().bind(echo)).expect("reservation echo must bind");
                (echo, bound)
            };
            assert!(slots.try_vacancy().is_err());
            assert!(builder.drain().bind(echo).is_none());
            assert_eq!(bound.value(), 23);
            if terminal {
                assert_eq!(bound.complete(), 23);
            }
            echo
        };
        assert!(builder.drain().bind(echo).is_none());
        let vacant =
            (slots.try_vacancy()).expect("consuming the binding releases the rolled-back entry");
        let next = vacant.reserve(29);
        assert!(next.echo() != echo);
        assert!(builder.drain().bind(echo).is_none());
    }
    assert_eq!(tracker.finish().allocations, 0);
}

#[test]
fn claims_completion_once() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1])).expect("one slot must prepare");
    let [slots] = prepared.groups();
    let (echo, bound) = {
        let vacant = (slots.try_vacancy()).expect("initial vacancy");
        let lease = vacant.reserve(31).commit();
        let echo = lease.echo();
        {
            let bound = (builder.drain().bind(echo)).expect("initial completion");
            assert!(builder.drain().bind(echo).is_none());
            assert_eq!(bound.value(), 31);
        }
        let bound = (builder.drain().bind(echo)).expect("dropping a binding retains the flight");
        (echo, bound)
    };
    assert!(slots.try_vacancy().is_err());
    assert_eq!(bound.complete(), 31);
    assert!(builder.drain().bind(echo).is_none());
    assert!(slots.try_vacancy().is_ok());
}

#[test]
fn rejects_terminal_echo() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1])).expect("one slot must prepare");
    let [slots] = prepared.groups();
    {
        let vacant = (slots.try_vacancy()).expect("initial vacancy");
        let lease = vacant.reserve(37).commit();
        let echo = lease.echo();
        assert_eq!(
            builder.drain().bind(echo).map(|bound| bound.complete()),
            Some(37)
        );
        assert_eq!(
            builder.drain().bind(echo).map(|bound| bound.complete()),
            None
        );
        assert_eq!(builder.drain().bind(echo).map(|bound| bound.value()), None);
        assert!(slots.try_vacancy().is_err());
        assert_eq!(lease.value(), 37);
    }
    assert!(slots.try_vacancy().is_ok());
}

#[test]
fn exhausts_bound_rollback() {
    let mut arena = storage::Flights::<u64, 32, 1>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1])).expect("one slot must prepare");
    let [slots] = prepared.groups();
    let echo = {
        let (echo, bound) = {
            let vacant = (slots.try_vacancy()).expect("the last generation must be usable");
            let reserved = vacant.reserve(41);
            let echo = reserved.echo();
            let bound = (builder.drain().bind(echo)).expect("the last generation must bind");
            (echo, bound)
        };
        assert!(matches!(slots.try_vacancy(), Err(Vacancy::Full)));
        assert_eq!(bound.value(), 41);
        echo
    };
    assert!(matches!(
        slots.try_vacancy(),
        Err(Vacancy::IdentityExhausted)
    ));
    assert!(builder.drain().bind(echo).is_none());
}

#[test]
fn binds_across_commit() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1])).expect("one slot must prepare");
    let [slots] = prepared.groups();
    for early in [false, true] {
        let vacant =
            (slots.try_vacancy()).expect("committing a bound reservation preserves recycling");
        let reserved = vacant.reserve(43);
        let echo = reserved.echo();
        let bound = (builder.drain().bind(echo)).expect("reservation echo must bind");
        if early {
            bound.finish();
            let lease = reserved.commit();
            assert_eq!(lease.value(), 43);
        } else {
            let lease = reserved.commit();
            assert!(builder.drain().bind(echo).is_none());
            assert_eq!(bound.complete(), lease.value());
        }
        assert!(builder.drain().bind(echo).is_none());
    }
}

#[test]
fn claims_prepared_groups() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1, 1])).expect("two reusable groups must prepare");
    let groups = prepared.groups();
    drop(groups);
    let tracker = Tracker::begin();
    {
        let left = (builder.try_batch([1])).expect("first reused group must prepare");
        let [left] = left.groups();
        let right = (builder.try_batch([1]))
            .expect("second reused group must prepare while the first remains leased");
        let [right] = right.groups();
        let left = (left.try_vacancy()).expect("first group retains its vacancy");
        let right = (right.try_vacancy()).expect("second group retains its vacancy");
        let left = left.reserve(47);
        let right = right.reserve(53);
        assert!(left.echo() != right.echo());
    }
    assert_eq!(tracker.finish().allocations, 0);
}

#[test]
fn rolls_back_preparation() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let prepared = (builder.try_batch([1])).expect("one reusable group must prepare");
    drop(prepared.groups());
    for allows in [0, 1] {
        let result = Tracker::fail_after(allows, || builder.try_batch([1, 4]));
        assert!(matches!(result, Err(o3::alloc::Error::Exhausted)));
        let tracker = Tracker::begin();
        let prepared =
            (builder.try_batch([1])).expect("failed preparation must return its claimed group");
        drop(prepared);
        assert_eq!(tracker.finish().allocations, 0);
    }
    let tracker = Tracker::begin();
    let prepared = (builder.try_batch([1])).expect("dropping preparation must release its group");
    let [slots] = prepared.groups();
    assert!(slots.try_vacancy().is_ok());
    assert_eq!(tracker.finish().allocations, 0);
}

#[test]
fn grows_while_bound() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let [first] = builder.try_batch([1]).expect("initial group").groups();
    let activity = {
        let lease = first
            .try_vacancy()
            .expect("initial slot")
            .reserve(59)
            .commit();
        let echo = lease.echo();
        let bound = builder.drain().bind(echo).expect("initial binding");
        let prepare = Tracker::begin();
        let prepared = builder.try_batch([2, 3]).expect("grow two groups");
        assert_eq!(prepare.finish().allocations, 3);
        let commit = Tracker::begin();
        let [second, third] = prepared.groups();
        assert_eq!(commit.finish().allocations, 0);
        let activity = Tracker::begin();
        let second = second
            .try_vacancy()
            .expect("second group")
            .reserve(61)
            .commit();
        let third = third
            .try_vacancy()
            .expect("third group")
            .reserve(67)
            .commit();
        assert_ne!(echo, second.echo());
        assert_ne!(echo, third.echo());
        assert_ne!(second.echo(), third.echo());
        assert!(builder.drain().bind(echo).is_none());
        assert_eq!(bound.complete(), 59);
        assert_eq!(lease.value(), 59);
        for flight in [second, third] {
            assert_eq!(
                builder
                    .drain()
                    .bind(flight.echo())
                    .expect("new echo")
                    .complete(),
                flight.value()
            );
        }
        activity
    };
    assert!(first.try_vacancy().is_ok());
    assert_eq!(activity.finish().allocations, 0);
}

#[test]
fn rolls_back_growth() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let [first] = builder.try_batch([1]).expect("initial group").groups();
    let lease = first
        .try_vacancy()
        .expect("initial slot")
        .reserve(71)
        .commit();
    let echo = lease.echo();
    for allows in [0, 1, 2] {
        {
            let result = Tracker::fail_after(allows, || builder.try_batch([2, 3]));
            assert!(matches!(result, Err(o3::alloc::Error::Exhausted)));
        }
        let bound = builder.drain().bind(echo).expect("old directory survives");
        assert_eq!(bound.value(), 71);
    }
    let pending = builder.try_batch([2, 3]).expect("droppable growth");
    drop(pending);
    let [second] = builder
        .try_batch([1])
        .expect("fresh growth after rollback")
        .groups();
    let next = second
        .try_vacancy()
        .expect("new index")
        .reserve(73)
        .commit();
    assert_eq!(next.echo().expose(), echo.expose() + 1);
    for flight in [lease, next] {
        assert_eq!(
            builder
                .drain()
                .bind(flight.echo())
                .expect("live echo")
                .complete(),
            flight.value()
        );
    }
}

#[test]
fn amortizes_growth() {
    let mut arena = storage::Flights::<u64>::new();
    let mut builder = arena.builder();
    let tracker = Tracker::begin();
    let groups: [_; 32] = array::from_fn(|_| {
        let [group] = builder.try_batch([1]).expect("retained group").groups();
        group
    });
    let activity = tracker.finish();
    assert_eq!(activity.allocations, 32 + 4);
    for (value, group) in groups.iter().enumerate() {
        let lease = group
            .try_vacancy()
            .expect("distinct slot")
            .reserve(value as u64)
            .commit();
        assert_eq!(
            builder
                .drain()
                .bind(lease.echo())
                .expect("grown directory")
                .complete(),
            value as u64
        );
    }
}
