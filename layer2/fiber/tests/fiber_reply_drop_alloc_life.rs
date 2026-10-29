use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_fiber::reply::{
    recv::attach::Error,
    usage::{self, Bytes, PushError, Usage, Values},
};
use dope_test::harness::scenario;

use crate::common::runtime::{self, Empty, Setup};
pub mod common;

use std::convert::Infallible;

use dope_core::io::Reclaimed;
use dope_fiber::{
    reply::{LaneIndex, Registration, recv, recv::attach::Attach as _},
    task::done::Once,
};
use o3::{alloc::array, base::cell::region, mem::credit::fair};

use crate::common::storage::replies::{
    life::*,
    support::{
        Cause, Counted, Drain, DrainFailure, Drops, Fixture, Maintenance, Never, Releases,
        RetainOne,
    },
};

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement failed: {:?}", failure.cause())
}

#[test]
fn deferral_holds_credit() {
    use std::{cell::Cell, pin::Pin, task::Poll};

    use dope::core::driver::route::{Epoch, KeyTag, indices::SlotIndex};
    use dope_fiber::{
        context::Context,
        reply::{Arena, layout},
    };

    use crate::common::storage::replies::support::limit;

    let extract_failure = Cell::new(false);
    let failures = Drops::new();
    let values = Drops::new();
    Setup::throughput().with_turn(
        |turn, mut driver| {
            let site = driver.driver_ref();
            let target = site
                .targets::<KeyTag<0>>()
                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                .dispatch();
            let ready = site.ready().make_slot(target).expect("reply wake slot");
            let layout = layout::Layout::single(layout::Entries::new(2), limit(1, 1, 1))
                .expect("reply layout");
            Arena::try_with(layout, driver.region_token().proof(), |arena| {
                let lane = arena.try_lane(LaneIndex::ZERO).expect("reply lane");
                let mut stream = recv::Stream::<_, Deferred>::with(&extract_failure);
                assert!(stream.try_attach(driver.region_token(), lane).is_ok());
                let mut failure = Counted::from(&Cause::new(7, &failures));
                failure.bytes = 1;
                assert!(lane.select(driver.region_token()).try_fail(failure).is_ok());
                let mut next = recv::Response::<_, Never>::new();
                assert!(next.try_attach(driver.region_token(), lane).is_ok());
                Context::with_target(
                    ready.target(),
                    turn.turn().app(),
                    driver.reborrow(),
                    |mut cx| {
                        assert!(
                            Pin::new(&mut stream).poll_next(cx.as_mut()).is_pending(),
                            "deferring extraction must preserve the pending failure"
                        );
                        assert_eq!(failures.count(), 0);
                        let (value, cause) = match lane
                            .select(cx.as_mut().region_token())
                            .try_push(Counted::mixed(&values))
                        {
                            Err(usage::InsertError::Backpressure { value, cause }) => {
                                Some((value, cause))
                            }
                            _ => None,
                        }
                        .expect("failure credit remains held before extraction");
                        let (resource, required, avail) = cause.into_parts();
                        assert_eq!(
                            (resource, required.get(), avail.get()),
                            (usage::Resource::Bytes, 1, 0)
                        );
                        extract_failure.set(true);
                        let failure = match Pin::new(&mut stream).poll_next(cx.as_mut()) {
                            Poll::Ready(value) => value,
                            Poll::Pending => None,
                        }
                        .expect("deferred failure must remain available");
                        assert_eq!(failure.failure, Some(7));
                        assert_eq!(failures.count(), 0);
                        assert!(
                            lane.select(cx.as_mut().region_token())
                                .try_push(value)
                                .is_ok()
                        );
                    },
                );
            })
            .expect("reply arena");
        },
        recovery::Policy::new(
            |event| assert!(matches!(event, Reclaimed::Shutdown)),
            recover,
        ),
        check_scope,
    );
    assert_eq!(failures.count(), 1);
}

#[test]
fn attach_credit() {
    let drops = Drops::new();

    region::Token::scope(|mut token| {
        Fixture::Lanes(&drops)
            .try_with(token.proof(), |state| {
                let state = &state;
                let arena = &state.arena;
                let lane = arena.try_lane(LaneIndex::ZERO);
                let lane = (lane).expect("lane exists");

                let mut first = recv::Response::<_, Never>::new();
                assert!(matches!(first.try_attach(&mut token, lane), Ok(())));
                let mut second = recv::Response::<_, Never>::new();
                let result = second.try_attach(&mut token, lane);
                assert!(matches!(
                    result,
                    Err(Error::Capacity {
                        lane: LaneIndex::ZERO,
                        cause: fair::Cause::Exhausted {
                            axis,
                            required,
                            available,
                        },
                    }) if axis.index() == 0 && required == 1 && available == 0
                ));
            })
            .expect("reply storage construction");
    });
}

#[test]
fn mismatch_rolls_back() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Slot(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = (arena.try_lane(LaneIndex::ZERO)).expect("lane exists");
                    {
                        let mut failed = recv::Response::<_, Never>::new();
                        assert!(
                            (failed.try_attach(session.driver().region_token(), lane)) == (Ok(()))
                        );
                        let (value, declared, actual) = match lane
                            .select(session.driver().region_token())
                            .try_build(Mismatch(&drops))
                        {
                            Err(PushError::Footprint {
                                value,
                                declared,
                                actual,
                            }) => Some((value, declared, actual)),
                            _ => None,
                        }
                        .expect("mismatched footprint must be rejected");
                        assert!(!value.is_data());
                        assert!((declared) == (Usage::new(Bytes::ZERO, Values::new(1))));
                        assert!((actual) == (Usage::ZERO));
                        lane.complete(session.driver().region_token());
                    }
                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize mismatched reply maintenance"
                    );
                    let released = releases.take();
                    assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));

                    let mut next = recv::Response::<_, Never>::new();
                    assert!((next.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_push(Counted::data(&drops))
                            .is_ok(),
                        "footprint contract failure must return its global credit"
                    );
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 2);
}

#[test]
fn terminal_follows_values() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Slot(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = (arena.try_lane(LaneIndex::ZERO)).expect("lane exists");
                    let mut response = recv::Response::<_, DrainFailure>::new();
                    assert!(
                        (response.try_attach(session.driver().region_token(), lane)) == (Ok(()))
                    );
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_push(Counted::data(&drops))
                            .is_ok()
                    );
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_push(Counted::data(&drops))
                            .is_ok()
                    );
                    let mut failure = Counted::from(&Cause::new(7, &drops));
                    failure.bytes = 2;
                    let (mut failure, cause) = match lane
                        .select(session.driver().region_token())
                        .try_fail(failure)
                    {
                        Err(usage::InsertError::Capacity { value, cause }) => Some((value, cause)),
                        _ => None,
                    }
                    .expect("terminal limit");
                    assert_eq!(
                        cause,
                        usage::Capacity::Limit {
                            resource: usage::Resource::Bytes,
                            current: usage::Current::new(0),
                            added: usage::Added::new(2),
                            limit: usage::Limit::new(1),
                        }
                    );
                    assert_eq!(drops.count(), 0);
                    failure.bytes = 0;
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_fail(failure)
                            .is_ok()
                    );

                    let outcome = ((session.with_quiesce(
                        &mut finish,
                        Empty {},
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(response)),
                    ))
                    .expect("finalize test app"))
                    .expect("receive failed reply");
                    assert_eq!(outcome, Ok((2, 7)));

                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize failed reply maintenance"
                    );
                    let released = releases.take();
                    assert!(released.into_iter().map(|lane| lane.index().get()).eq([0]));
                    let mut next = recv::Response::<_, Never>::new();
                    assert!((next.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 3);
}

#[test]
fn reuse_waits_for_owner() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Slot(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let lane = (arena.try_lane(LaneIndex::ZERO)).expect("lane exists");
                    let mut response = recv::Response::<_, RetainOne>::new();
                    assert!(
                        (lane.control().registration(session.driver().region_token()))
                            == (Registration::Available)
                    );
                    assert!(
                        (response.try_attach(session.driver().region_token(), lane)) == (Ok(()))
                    );
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_push(Counted::data(&drops))
                            .is_ok()
                    );
                    lane.complete(session.driver().region_token());

                    let releases = Releases::new();
                    {
                        let result = session.with_quiesce(
                            &mut finish,
                            Empty {},
                            scenario::TIMEOUT,
                            |mut app| app.drive(Once::new(response)),
                        );
                        assert!(
                            result.is_ok(),
                            "finalize test app: {:?}",
                            result.as_ref().err().map(|error| error.cause())
                        );
                        let retained = result
                            .ok()
                            .expect("finalized test app")
                            .expect("receive retained response")
                            .expect("reply retention");

                        let mut blocked = recv::Response::<_, Never>::new();
                        assert!(
                            (lane.control().registration(session.driver().region_token()))
                                == (Registration::Full)
                        );
                        let result = blocked.try_attach(session.driver().region_token(), lane);
                        assert!(matches!(
                            result,
                            Err(Error::Full { len, capacity })
                                if len.get() == 1 && capacity.get() == 1
                        ));
                        let (value, held) = retained.into_parts();
                        assert!(value.is_data());
                        let (bytes, values) = held.into_parts();
                        assert!(bytes.is_none());
                        assert!(values.is_some());
                    }
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize retained owner maintenance"
                    );
                    let mut next = recv::Response::<_, Never>::new();
                    assert!(
                        (lane.control().registration(session.driver().region_token()))
                            == (Registration::Available)
                    );
                    assert!((next.try_attach(session.driver().region_token(), lane)) == (Ok(())));
                    let released = releases.take();
                    assert!(
                        released.into_iter().map(|lane| lane.index().get()).eq([0]),
                        "retained-owner retirement reports its exact lane"
                    );
                    assert!(
                        lane.select(session.driver().region_token())
                            .try_push(Counted::data(&drops))
                            .is_ok()
                    );
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(drops.count(), 2);
}

#[test]
fn releases_stay_distinct() {
    let mut finish = Completion::new();
    let drops = Drops::new();
    let mut reports = Vec::with_capacity(1);

    Setup::throughput().with_session(
        |mut session| {
            let proof = session.driver().region_token().proof();
            Fixture::Lanes(&drops)
                .try_with(proof, |state| {
                    let state = &state;
                    let arena = &state.arena;
                    let left = (arena.try_lane(LaneIndex::ZERO)).expect("left lane");
                    let right = (arena.try_lane(LaneIndex::new::<1>())).expect("right lane");
                    {
                        let mut right_rx = recv::Response::<_, Never>::new();
                        let mut left_rx = recv::Response::<_, Never>::new();
                        assert!(
                            (left_rx.try_attach(session.driver().region_token(), left)) == (Ok(()))
                        );
                        assert!(
                            (right_rx.try_attach(session.driver().region_token(), right))
                                == (Ok(()))
                        );
                        left.complete(session.driver().region_token());
                        right.complete(session.driver().region_token());
                    }

                    let releases = Releases::new();
                    let result = session.with_quiesce(
                        &mut finish,
                        Maintenance::new(state, &releases),
                        scenario::TIMEOUT,
                        |mut app| app.drive(Once::new(Drain::new(state))),
                    );
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "finalize multi-lane reply maintenance"
                    );
                    let entries = array::Block::try_new(arena.domain(), ())
                        .expect("lane entries")
                        .init(|index| index);
                    let released = releases.take().into_iter();
                    assert!(released.map(|lane| *lane.entry(&entries)).eq([0, 1]));
                })
                .expect("reply storage construction");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        check_scope,
    );
    assert!(
        matches!(reports.as_slice(), [] | [Reclaimed::Shutdown]),
        "unexpected reclaimed operations: {reports:?}"
    );
}
