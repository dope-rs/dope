pub mod common;

use std::{
    cell::Cell, convert::Infallible, fmt::Debug, io::Write as _, marker::PhantomData, mem, rc::Rc,
};

use common::{
    manifold::listener::terminal::{
        Ingress, Observation, Probe,
        wire::{self, CleanOnData, Event, FailOnData, FailOnEof, Fault, StallOnData, Wire},
    },
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::listener::handler::failures,
    timing::Throughput,
};
use dope_net::tcp::Tcp;
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers::{Run, Wait},
    harness::{
        peer::{Half, Peer},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn terminal(event: Observation<'_>) {
    use dope_manifold::network::listener::conn::{
        Failure,
        deadline::Arm,
        write::{self, body::Admit},
    };
    assert!(match event {
        Observation::Deadline(armed) => matches!(armed, Arm::Armed),
        Observation::Write(result) => matches!(result, Ok(write::Outcome::Committed(()))),
        Observation::Owned(result) => matches!(result, Admit::Committed),
        Observation::Replay(result) => matches!(result, Some(Ok(()))),
        Observation::Failure(cause) => matches!(
            cause,
            Some(Failure::Write(write::body::Failure::Capacity(
                write::Reject::Header
            )))
        ),
    });
}

#[test]
fn keeps_write_failure() {
    let mut finish = Completion::new();
    let rejected = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<FailOnEof>, Throughput>>((
            Probe::<Wire<FailOnEof>, receive::Coupled, _> {
                close: Gate::new(),
                retired: retired.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Rejected(rejected.clone()),
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let mut peer = Peer::at(case.addr()).try_connect().expect("peer");
                peer.write_all(b"reject").expect("write trigger");
                reached(case.try_wait(&rejected, 1), rejected.hits(), 1);
                assert_eq!(retired.hits(), 0);
                Peer::try_reset(peer).expect("reset rejected connection");
                reached(case.try_wait(&retired, 1), retired.hits(), 1);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(reports.events().expect("reclaimed events").is_empty());
}

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
fn hard_abort_wins() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<FailOnEof>, Throughput>>((
            Probe::<Wire<FailOnEof>, receive::Coupled, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Fail,
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .with(
                        |stream| Half::try_run(stream, b"abort before eof observation"),
                        |peer| {
                            reached(case.try_wait(&close, 1), close.hits(), 1);
                            reached(case.try_wait(&done, 1), done.hits(), 1);
                            assert!(
                                peer.finish()
                                    .expect("peer finish")
                                    .expect("peer I/O")
                                    .is_empty()
                            );
                        },
                    )
                    .expect("peer spawn");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(wire.eof_count(), 0);
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"abort before eof observation".to_vec()),
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn app_failure_wins() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<CleanOnData>, Throughput>>((
            Probe::<Wire<CleanOnData>, receive::Coupled, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Fail,
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"clean terminal application failure".to_vec(), |peer| {
                        reached(case.try_wait(&close, 1), close.hits(), 1);
                        reached(case.try_wait(&done, 1), done.hits(), 1);
                        assert!(
                            peer.finish()
                                .expect("peer finish")
                                .expect("peer I/O")
                                .is_empty()
                        );
                    })
                    .expect("peer spawn");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"clean terminal application failure".to_vec()),
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn drains_scoped_tail() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Scoped>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<FailOnData>, Throughput>>((
            Probe::<Wire<FailOnData>, receive::Scoped, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Close,
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"scoped terminal".to_vec(), |peer| {
                        reached(case.try_wait(&close, 1), close.hits(), 1);
                        reached(case.try_wait(&done, 1), done.hits(), 1);
                        assert_eq!(peer.finish().expect("peer finish").expect("peer I/O"), b"!");
                    })
                    .expect("peer spawn");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"scoped terminal".to_vec()),
                Event::ControlPrepared,
                Event::Chunk(b"scoped terminal".to_vec()),
                Event::ControlSent,
                Event::Wire(Fault::Protocol, None, 0),
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn sends_control_first() {
    control::<false>(None);
}

#[test]
fn keeps_closing_failure() {
    control::<true>(Some(Fault::Closing));
}

fn control<const FAIL: bool>(closing: Option<Fault>) {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<FailOnData<FAIL>>, Throughput>>((
            Probe::<Wire<FailOnData<FAIL>>, receive::Coupled, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Fail,
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"application abort".to_vec(), |peer| {
                        reached(case.try_wait(&close, 1), close.hits(), 1);
                        reached(case.try_wait(&done, 1), done.hits(), 1);
                        assert_eq!(peer.finish().expect("peer finish").expect("peer I/O"), b"!");
                    })
                    .expect("peer spawn");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"application abort".to_vec()),
                Event::ControlPrepared,
                Event::ControlSent,
                Event::Wire(Fault::Protocol, closing, 0),
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn abandons_stalled_control() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<StallOnData>, Throughput>>((
            Probe::<Wire<StallOnData>, receive::Coupled, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Close,
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"stalled terminal control".to_vec(), |peer| {
                        reached(case.try_wait(&close, 1), close.hits(), 1);
                        reached(case.try_wait(&done, 1), done.hits(), 1);
                        assert!(
                            peer.finish()
                                .expect("peer finish")
                                .expect("peer I/O")
                                .is_empty()
                        );
                    })
                    .expect("peer spawn");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"stalled terminal control".to_vec()),
                Event::Wire(Fault::Protocol, None, 0),
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn close_layout() {
    assert_eq!(mem::size_of::<failures::Close>(), 0);
    assert!(!mem::needs_drop::<failures::Close>());
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
