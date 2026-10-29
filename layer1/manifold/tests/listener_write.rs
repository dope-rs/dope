pub mod common;

use std::{cell::Cell, convert::Infallible, fmt::Debug, rc::Rc};

use dope_core::driver::life::recovery;
use dope_runtime::topology::error::Kind;
use dope_test::fibers::{Run, Wait};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use common::{
    manifold::listener::pipeline::{
        admit, body,
        prefix::{Admission, Attempt, Prefix, Seen, State},
        wake::{self, Wake, Zero, ZeroEvent},
    },
    scenario::case::listener::Listener,
};
use dope_core::driver::route::{FrameworkTag, KeyTag, Tag, kind};
use dope_manifold::{app::env::Bundle, network::listener::conn::write, timing::Throughput};
use dope_net::{link::egress, tcp::Tcp, wire::Identity};
use dope_runtime::executor::session::Completion;
use dope_test::{
    alloc::Tracker,
    harness::{
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn native<R: Debug>(outcome: Result<R, Kind<'_, Infallible, &R>>) -> R {
    outcome.expect("native lifecycle")
}

fn reached(outcome: Result<Wait, Run>, actual: u32, want: u32) {
    assert_eq!(
        outcome.expect("drive progress"),
        Wait::Reached,
        "{actual} of {want} hits"
    );
}

#[test]
fn queued_writes_survive() {
    let mut finish = Completion::new();
    let mut want = Vec::new();
    want.extend_from_slice(admit::FIRST_HEADER);
    want.extend_from_slice(admit::FIRST_BODY);
    want.extend_from_slice(admit::SECOND_HEADER);
    want.extend_from_slice(admit::SECOND_BODY);
    want.extend_from_slice(admit::THIRD_HEADER);
    want.extend_from_slice(admit::THIRD_BODY);
    want.extend_from_slice(admit::FIRST_REPLY.as_ref());
    want.extend_from_slice(admit::SECOND_REPLY.as_ref());
    let gate = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let prepared = Listener::new::<64>(Default::default())
        .direct(0)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            admit::Admit {
                gate: gate.clone(),
                events: Rc::clone(&events),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"GO\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        let got = peer
                            .finish()
                            .expect("finish client peer")
                            .expect("client peer failed");
                        assert_eq!(got, want);
                        assert_eq!(gate.hits(), 1);
                    })
                    .expect("spawn admission peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(matches!(events.take().as_slice(), [
        admit::Event::Chunk(len),
        admit::Event::Write(Ok(write::Outcome::Committed(())), egress::Pending::Present),
        writes @ ..,
    ] if *len != 0 && writes.len() == 4 && writes.iter().all(|event| matches!(event, admit::Event::Write(Ok(write::Outcome::Committed(())), _)))));
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn bounded_reports_attempt() {
    let mut finish = Completion::new();
    let called = Rc::new(Cell::new(false));
    let attempts = Rc::new(Cell::new(Vec::new()));
    let gate = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(0)
        .egress(egress::Config::shared(egress::Credit::fixed::<
            1,
            { write::Prefix::CAPACITY as u32 },
        >()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            admit::Bounded {
                called: called.clone(),
                gate: gate.clone(),
                attempts: Rc::clone(&attempts),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"GO\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        assert!(
                            peer.finish()
                                .expect("finish client peer")
                                .expect("client peer failed")
                                .is_empty()
                        );
                        assert!(called.get());
                    })
                    .expect("spawn admission peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    let attempts = attempts.take();
    assert!(
        !attempts.is_empty()
            && attempts
                .iter()
                .all(|attempt| matches!(attempt, admit::Attempt {
        input,
        state: 9,
        outcome: admit::BoundedWrite::Draft {
            cause: write::error::Bounded::Geometry(egress::reserve::error::Full::Lane(_)),
            written: 1,
            body: Some(body),
            end: write::End::Open,
        },
    } if *input != 0 && body == admit::FIRST_BODY))
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn direct_recycles_bodies() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let prepared = Listener::new::<8>(Default::default())
        .direct(1)
        .egress(egress::Config::shared(egress::Credit::fixed::<0, 0>()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            body::Body {
                gate: gate.clone(),
                events: Rc::clone(&events),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"GO\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        let got = peer
                            .finish()
                            .expect("finish client peer")
                            .expect("client peer failed");
                        let want: Vec<u8> = [
                            body::FIRST.as_slice(),
                            body::SECOND.as_slice(),
                            body::THIRD.as_slice(),
                        ]
                        .into_iter()
                        .flatten()
                        .copied()
                        .collect();
                        assert_eq!(got, want);
                    })
                    .expect("peer spawn failed");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    use body::{Event, State};
    let events = events.take();
    assert!(events.iter().all(|event| !matches!(event, Event::Chunk(0))));
    let steps: Vec<_> = events
        .iter()
        .filter(|event| !matches!(event, Event::Chunk(_)))
        .collect();
    assert!(matches!(
        steps.as_slice(),
        [
            Event::Submit(State::First),
            Event::Write(
                Some(write::Prefix::CAPACITY),
                Ok(write::Outcome::Committed(()))
            ),
            Event::Submit(State::Second),
            Event::Write(
                Some(write::Prefix::CAPACITY),
                Ok(write::Outcome::Committed(()))
            ),
            Event::Submit(State::Third),
            Event::Write(
                Some(write::Prefix::CAPACITY),
                Ok(write::Outcome::Committed(()))
            ),
            Event::Close(State::Done),
        ]
    ));
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn changed_body_commits() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let commits = Rc::new(Cell::new(0));
    let defers = Rc::new(Cell::new(0));
    let wakes = Rc::new(Cell::new(0));
    let events = Rc::new(Cell::new(Vec::new()));
    let prepared = Listener::new::<1>(Default::default())
        .direct(1)
        .egress(egress::Config::shared(egress::Credit::fixed::<
            1,
            { 16 * 1024 },
        >()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Wake {
                gate: gate.clone(),
                commits: commits.clone(),
                defers: defers.clone(),
                wakes: wakes.clone(),
                changed: Cell::new(false),
                events: Rc::clone(&events),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"GO\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        assert_eq!(
                            peer.finish()
                                .expect("finish client peer")
                                .expect("client peer failed"),
                            b"ABC"
                        );
                        assert_eq!(commits.get(), 3);
                        assert!(defers.get() >= 1);
                        assert!(wakes.get() >= 1);
                    })
                    .expect("peer spawn failed");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    let events = events.take();
    assert!(
        !events.is_empty()
            && events.iter().all(|event| {
                match event {
                    wake::Event::Chunk(len) => *len != 0,
                    wake::Event::Write(callback, result) => {
                        callback.is_none_or(|(capacity, state)| {
                            capacity == write::Prefix::CAPACITY && state == 2
                        }) && matches!(
                            result,
                            Ok(write::Outcome::Committed(()) | write::Outcome::Deferred(_))
                        )
                    }
                }
            })
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn direct_zero_rolls_back() {
    bounded_zero_rolls_back(1);
}

#[test]
fn queued_zero_rolls_back() {
    bounded_zero_rolls_back(0);
}

fn bounded_zero_rolls_back(direct: usize) {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let attempts = Rc::new(Cell::new(Vec::new()));
    let prepared = Listener::new::<1>(Default::default())
        .direct(direct)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Zero {
                gate: gate.clone(),
                attempts: Rc::clone(&attempts),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"GO\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        assert_eq!(
                            peer.finish()
                                .expect("finish client peer")
                                .expect("client peer failed"),
                            b"Z"
                        );
                    })
                    .expect("peer spawn failed");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    let attempts = attempts.take();
    assert!(
        !attempts.is_empty()
            && attempts.chunks(2).all(|events| matches!(events, [
        ZeroEvent::Zero {
            input, before, capacity: Some(write::Prefix::CAPACITY),
            result: Ok(write::Outcome::Committed(7)), allocations: (0, 0),
            remaining, pending: egress::Pending::Absent,
        },
        ZeroEvent::Sent {
            result: Ok(write::Outcome::Committed(())), allocations: (0, 0),
            remaining: after,
        },
    ] if *input != 0 && *before != 0 && remaining == before && after + 1 == *before))
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reserves_before_callback() {
    let seen = Rc::new(Cell::new(Seen::default()));
    let gate = Gate::new();
    let mut finish = Completion::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(0)
        .egress(egress::Config::shared(egress::Credit::fixed::<
            1,
            { 2 * write::Prefix::CAPACITY as u32 },
        >()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Prefix {
                seen: seen.clone(),
                gate: gate.clone(),
            },
            Identity,
        ))
        .expect("prefix listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"GO\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        let bytes = peer
                            .finish()
                            .expect("join prefix peer")
                            .expect("prefix wire");
                        assert_eq!(bytes, b"AB");
                    })
                    .expect("prefix peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    let seen = seen.take();
    assert!(seen.chunks.iter().all(|len| *len != 0));
    assert!(matches!(
        seen.admissions.as_slice(),
        [(Admission::Committed, egress::Pending::Present)]
    ));
    assert!(matches!(seen.attempts.as_slice(), [deferred @ .., Attempt {
        callback: Some(State::Queued), state: State::Done, allocations: (0, 0),
        result: Ok(write::Outcome::Committed(())),
    }] if !deferred.is_empty() && deferred.iter().all(|attempt| matches!(attempt, Attempt {
        callback: None, state: State::Queued, allocations: (0, 0),
        result: Ok(write::Outcome::Deferred(write::Deferred::Capacity)),
    }))));
    assert_eq!(seen.closed, Some(State::Done));
    assert_eq!(gate.hits(), 1);
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "{reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
