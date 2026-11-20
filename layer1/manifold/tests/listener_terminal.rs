pub mod common;

use common::manifold::listener::reached;

use std::{cell::Cell, marker::PhantomData, rc::Rc};

use common::{
    manifold::listener::terminal::{
        Ingress, Observation, Probe,
        wire::{self, Event, Fault, Mode, Wire},
    },
    scenario::case::listener::Listener,
};
use dope_core::driver::route::KeyTag;
use dope_manifold::{
    app::{env::Bundle, receive},
    timing::Throughput,
};
use dope_net::tcp::Tcp;
use dope_test::harness::{
    GUARD,
    peer::{Half, Peer},
    signals::{Gate, Progress as _},
};

fn terminal(event: Observation<'_>) {
    use dope_manifold::network::listener::conn::{
        Failure,
        write::{self, body::Admit},
    };
    assert!(match event {
        Observation::Deadline(armed) => armed.is_ok(),
        Observation::Write(result) => matches!(result, Ok(())),
        Observation::Owned(result) => matches!(result, Admit::Committed),
        Observation::Replay => true,
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
    let rejected = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events), Mode::FailOnEof));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Probe::<receive::Coupled, _> {
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
    prepared.run(|case| {
        let peer = Peer::at(case.addr()).try_connect().expect("peer");
        peer.write_all(b"reject").expect("write trigger");
        reached(case.try_wait(&rejected, 1), rejected.hits(), 1);
        assert_eq!(retired.hits(), 0);
        Peer::try_reset(peer).expect("reset rejected connection");
        reached(case.try_wait(&retired, 1), retired.hits(), 1);
    });
}

#[test]
fn hard_abort_wins() {
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events), Mode::FailOnEof));
    let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Probe::<receive::Coupled, _> {
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
    prepared.run(|case| {
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
    });
    wire.check();
    assert_eq!(wire.eof_count(), 0);
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"abort before eof observation".to_vec()),
                Event::Application(receive::Failure::Contract),
                Event::Close,
                Event::Application(receive::Failure::Contract),
                Event::Retired,
            ]
    );
}

#[test]
fn app_failure_wins() {
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events), Mode::CleanOnData));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Probe::<receive::Coupled, _> {
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
    prepared.run(|case| {
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
    });
    wire.check();
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"clean terminal application failure".to_vec()),
                Event::Application(receive::Failure::Contract),
                Event::Close,
                Event::Application(receive::Failure::Contract),
                Event::Retired,
            ]
    );
}

#[test]
fn keeps_accept_failure() {
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events), Mode::CleanOnData));
    let cause = receive::Failure::ParseCapacity;
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Probe::<receive::Coupled, _> {
                close: Gate::new(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::AcceptFailure(cause),
                marker: PhantomData,
                observe: terminal,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    prepared.run(|case| {
        let peer = Peer::at(case.addr()).try_read(GUARD).expect("peer");
        reached(case.try_wait(&done, 1), done.hits(), 1);
        assert!(Peer::try_read_all(&peer).expect("peer EOF").is_empty());
    });
    wire.check();
    assert!(
        events.take().as_slice()
            == [
                Event::Application(cause),
                Event::Close,
                Event::Application(cause),
                Event::Retired,
            ]
    );
}

#[test]
fn drains_scoped_tail() {
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(
        Rc::clone(&events),
        Mode::FailOnData { closing: false },
    ));
    let prepared = Listener::<receive::Scoped>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Probe::<receive::Scoped, _> {
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
    prepared.run(|case| {
        Peer::at(case.addr())
            .exchange(b"scoped terminal".to_vec(), |peer| {
                reached(case.try_wait(&close, 1), close.hits(), 1);
                reached(case.try_wait(&done, 1), done.hits(), 1);
                assert_eq!(peer.finish().expect("peer finish").expect("peer I/O"), b"!");
            })
            .expect("peer spawn");
    });
    wire.check();
    let events = events.take();
    let [
        Event::Chunk(first),
        Event::ControlPrepared,
        left,
        right,
        Event::Wire(Fault::Protocol, None, 0),
        Event::Close,
        Event::Retired,
    ] = events.as_slice()
    else {
        panic!("scoped terminal events: {events:?}");
    };
    assert_eq!(first, b"scoped terminal");
    assert!(
        matches!(
            (left, right),
            (Event::Chunk(tail), Event::ControlSent) | (Event::ControlSent, Event::Chunk(tail))
                if tail == first
        ),
        "scoped terminal events: {events:?}",
    );
}

#[test]
fn sends_control_first() {
    for closing in [None, Some(Fault::Closing)] {
        let close = Gate::new();
        let done = Gate::new();
        let events = Rc::new(Cell::new(Vec::new()));
        let wire = Rc::new(wire::Probe::new(
            Rc::clone(&events),
            Mode::FailOnData {
                closing: closing.is_some(),
            },
        ));
        let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
                Probe::<receive::Coupled, _> {
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
        prepared.run(|case| {
            Peer::at(case.addr())
                .exchange(b"application abort".to_vec(), |peer| {
                    reached(case.try_wait(&close, 1), close.hits(), 1);
                    reached(case.try_wait(&done, 1), done.hits(), 1);
                    assert_eq!(
                        peer.finish().expect("peer finish").expect("peer I/O"),
                        b"!"
                    );
                })
                .expect("peer spawn");
        });
        wire.check();
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
}

#[test]
fn abandons_stalled_control() {
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events), Mode::StallOnData));
    let prepared = Listener::<receive::Coupled>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Probe::<receive::Coupled, _> {
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
    prepared.run(|case| {
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
    });
    wire.check();
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
