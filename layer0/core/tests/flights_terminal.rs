use dope_core::driver::retained::flight::storage::{Echo, Flights};
use dope_sys::io::reactor::complete::flags::Flow;
use o3::depot::arena::slab::Vacancy;

#[test]
fn rolls_back_reservation() {
    let mut arena = Flights::<u64>::new();
    let mut builder = arena.builder();
    let echo = {
        let lease = {
            let prepared =
                (builder.try_batch([1])).expect("one flight slot must prepare successfully");
            let [slots] = prepared.groups();
            let previous = {
                let vacant = (slots.try_vacancy()).expect("first entry");
                let res = vacant.reserve(11);
                assert!(slots.try_vacancy().is_err());
                res.echo()
            };
            let vacant = (slots.try_vacancy()).expect("rolled-back entry");
            let res = vacant.reserve(17);
            assert!(res.echo() != previous);
            res.commit()
        };
        let echo = lease.echo();
        assert_eq!(
            builder.drain().bind(echo).map(|bound| bound.complete()),
            Some(17)
        );
        assert_eq!(lease.value(), 17);
        echo
    };

    let prepared = (builder.try_batch([1])).expect("the released flight slot must prepare again");
    let [reused] = prepared.groups();
    let vacant = (reused.try_vacancy()).expect("recycled entry");
    let res = vacant.reserve(19);
    assert!(res.echo() != echo);
}

#[test]
fn waits_for_terminal() {
    let mut arena = Flights::<u64>::new();
    let mut builder = arena.builder();
    let echo = {
        let lease = {
            let prepared =
                (builder.try_batch([1])).expect("one flight slot must prepare successfully");
            let [slots] = prepared.groups();
            let vacant = (slots.try_vacancy()).expect("flight entry");
            vacant.reserve(23).commit()
        };
        lease.echo()
    };

    assert_eq!(
        builder.drain().bind(echo).map(|bound| bound.value()),
        Some(23)
    );

    {
        let prepared =
            (builder.try_batch([1])).expect("the detached flight must admit an independent group");
        let [group] = prepared.groups();
        let vacant = (group.try_vacancy()).expect("independent group");
        let res = vacant.reserve(29);
        assert!(res.echo() != echo);
    }

    assert_eq!(
        builder.drain().bind(echo).map(|bound| bound.complete()),
        Some(23)
    );
}

#[test]
fn distinguishes_exhaustion() {
    let mut arena = Flights::<u64, 32, 1>::new();
    let mut builder = arena.builder();
    let [slots] = builder.try_batch([3]).expect("rounded group").groups();
    let first = slots.try_vacancy().expect("first identity").reserve(1);
    let second = slots
        .try_vacancy()
        .expect("second identity")
        .reserve(2)
        .commit();
    let held = slots.try_vacancy().expect("held vacancy");
    drop(first);
    assert!(matches!(slots.try_vacancy(), Err(Vacancy::Full)));

    let echo = second.echo();
    drop(second);
    builder
        .drain()
        .bind(echo)
        .expect("detached flight")
        .finish();
    drop(held);
    let last = slots
        .try_vacancy()
        .expect("unspent held identity")
        .reserve(3)
        .commit();
    let echo = last.echo();
    builder
        .drain()
        .bind(echo)
        .expect("attached flight")
        .finish();
    assert!(matches!(slots.try_vacancy(), Err(Vacancy::Full)));
    drop(last);
    assert!(matches!(
        slots.try_vacancy(),
        Err(Vacancy::IdentityExhausted)
    ));
    assert!(builder.drain().bind(echo).is_none());

    drop(slots);
    let [empty] = builder.try_batch([0]).expect("empty reused group").groups();
    assert!(matches!(empty.try_vacancy(), Err(Vacancy::Full)));
}

#[test]
fn round_trips_echo() {
    let mut arena = Flights::<u64>::new();
    let mut builder = arena.builder();
    let lease = {
        let prepared = (builder.try_batch([1])).expect("one flight slot must prepare successfully");
        let [slots] = prepared.groups();
        let vacant = (slots.try_vacancy()).expect("flight entry");
        vacant.reserve(31).commit()
    };
    let echo = lease.echo();
    let raw = echo.expose();

    let back = (Echo::<u64>::from_exposed(raw)).expect("an exposed live echo remains valid");
    assert!(back == echo);
    assert!(Echo::<u64>::from_exposed(0).is_none());
    assert!(Echo::<u64>::from_exposed(1).is_none());

    assert_eq!(
        builder.drain().bind(back).map(|bound| bound.complete()),
        Some(31)
    );
    assert_eq!(lease.value(), 31);
}

#[test]
fn continuations_keep_flights() {
    let mut arena = Flights::<u64>::new();
    let mut builder = arena.builder();
    let [slots] = builder.try_batch([1]).expect("one flight slot").groups();
    for attached in [true, false] {
        {
            let (echo, lease) = {
                let lease = slots
                    .try_vacancy()
                    .expect("vacant flight")
                    .reserve(73)
                    .commit();
                let echo = lease.echo();
                if attached {
                    (echo, Some(lease))
                } else {
                    (echo, None)
                }
            };
            for (flow, pending) in [(Flow::More, true), (Flow::More, true), (Flow::Last, false)] {
                let bound = builder.drain().bind(echo).expect("original pending echo");
                assert_eq!(bound.settle(flow.into()), 73);
                if let Some(lease) = &lease {
                    assert_eq!(lease.is_pending(), pending);
                }
                assert_eq!(slots.try_vacancy().is_err(), attached || pending);
            }
        }
        assert!(
            slots.try_vacancy().is_ok(),
            "only the last packet permits reuse"
        );
    }
}
