pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt::Debug,
    io::{self, Read as _, Write as _},
    marker::PhantomData,
    net::Shutdown,
    rc::Rc,
    time::{Duration, Instant},
};

use common::{
    manifold::listener::{
        Cancel, CancelAt, Trace,
        terminal::{
            Ingress, Probe, accepted,
            wire::{self, CleanOnEof, Event, FailOnEof, Fault, Wire},
        },
        trace::One,
    },
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{FrameworkTag, KeyTag, Tag, kind},
    settings::claim::Needs,
};
use dope_manifold::{
    app::{Host, env::Bundle, receive},
    network::listener::{self, topology::Layout},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::domain};
use dope_runtime::{
    executor::{
        Executor,
        session::{self, Completion},
    },
    topology::error::Kind,
};
use dope_test::{
    fibers::{self, Run, Wait},
    harness::{
        GUARD,
        observe::VecCellExt as _,
        peer::{Half, Peer},
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
fn shutdown_keeps_close() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let cancel = Gate::new();
    let trace = Rc::new(Trace::default());
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let server = One::config();
    let layout = Layout::<KeyTag<0>, receive::Coupled>::new(server.connections);
    let config = Needs::from_tree(&layout)
        .expect("listener requirements")
        .resolve::<Throughput>()
        .expect("listener layout");
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, config);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("create executor");
    let executor = executor.ok().expect("executor");
    let mut reports = Reports::<_>::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    executor.run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let hash = session.hash_state(listener::Domain::DEFAULT);
                listener::Install::<
                        KeyTag<0>,
                        Bundle<Tcp, Wire<CleanOnEof<true>>, Throughput>,
                        _,
                    >::new(server, domain::Local::new(Rc::clone(&wire)))
                    .with_driver(
                        Probe::<Wire<CleanOnEof<true>>, receive::Coupled, _> {
                            close: close.clone(),
                            retired: done.clone(),
                            events: Rc::clone(&events),
                            ingress: Ingress::Held,
                            marker: PhantomData,
            observe: accepted,
                        },
                        hash,
                        &mut session,
                        session::Scope::driver,
                        |listener, session| {
                            let addr = listener.local_addr();
                            let result = session.with_quiesce(
                                &mut finish,
                                Host::new(Cancel {
                                    listener,
                                    at: CancelAt::Send,
                                    trace: Rc::clone(&trace),
                                    gate: cancel.clone(),
                                }),
                                Duration::from_secs(2),
                                |mut app| {
                                    Peer::at(addr)
                                        .with(
                                            |stream| Half::try_run(stream, b"x"),
                                            |peer| {
                                                reached(
                                                    fibers::TEST.try_wait(&mut app, &close, 1),
                                                    close.hits(),
                                                    1,
                                                );
                                                reached(
                                                    fibers::TEST.try_wait(&mut app, &done, 1),
                                                    done.hits(),
                                                    1,
                                                );
                                                assert_eq!(
                                                    peer.finish()
                                                        .expect("peer finish")
                                                        .expect("peer I/O"),
                                                    b"!"
                                                );
                                            },
                                        )
                                        .expect("peer spawn");
                                },
                            );
                            assert!(result.is_ok(), "finish clean control application");
                        },
                    )
                    .expect("install listener");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "finish clean control domain");
    });
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(cancel.hits(), 1);
    wire.check();
    assert!(
        trace
            .send_shutdowns
            .take()
            .iter()
            .all(
                |(capacity, remaining)| *capacity == o3::depot::capacity::Count::new(1)
                    && *remaining >= 1
            )
    );
    assert_eq!(wire.eof_count(), 1);
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"x".to_vec()),
                Event::Eof,
                Event::ControlPrepared,
                Event::ControlSent,
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn half_close_before_gate() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let peer_eof = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<CleanOnEof<true>>, Throughput>>((
            Probe::<Wire<CleanOnEof<true>>, receive::Coupled, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::HalfClose(peer_eof.clone()),
                marker: PhantomData,
                observe: accepted,
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
                let mut peer = Peer::at(case.addr()).try_connect().expect("connect peer");
                peer.write_all(b"x").expect("write request");
                peer.shutdown(Shutdown::Write).expect("peer half-close");
                peer.set_nonblocking(true).expect("nonblocking peer");
                let started = Instant::now();
                let mut reply = Vec::new();
                let mut bytes = [0; 64];
                loop {
                    assert!(
                        started.elapsed() < GUARD,
                        "peer did not observe EOF before gate release"
                    );
                    match peer.read(&mut bytes) {
                        Ok(0) => break,
                        Ok(count) => reply.extend_from_slice(&bytes[..count]),
                        Err(error) => {
                            assert!(
                                matches!(
                                    error.kind(),
                                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                                ),
                                "read peer EOF: {error}"
                            );
                            case.try_drive(fibers::Yield::new())
                                .expect("drive peer EOF");
                        }
                    }
                }
                assert_eq!(reply, b"!");
                assert_eq!(close.hits(), 0, "EOF must precede gate release and close");
                assert_eq!(done.hits(), 0, "EOF must precede retirement");
                peer_eof.hit();
                reached(case.try_wait(&close, 1), close.hits(), 1);
                reached(case.try_wait(&done, 1), done.hits(), 1);
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
    assert_eq!(wire.eof_count(), 1);
    assert_eq!(peer_eof.hits(), 1);
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"x".to_vec()),
                Event::Eof,
                Event::ControlPrepared,
                Event::ControlSent,
                Event::ShutdownRequested,
                Event::PeerEof,
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn eof_waits_for_release() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<CleanOnEof>, Throughput>>((
            Probe::<Wire<CleanOnEof>, receive::Coupled, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Hold,
                marker: PhantomData,
                observe: accepted,
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
                        |stream| Half::try_run(stream, b"x"),
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
    assert_eq!(wire.eof_count(), 1);
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"x".to_vec()),
                Event::Eof,
                Event::Timeout,
                Event::Close,
                Event::Retired,
            ]
    );
}

#[test]
fn publishes_eof() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let done = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Gated>::new::<4>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<FailOnEof>, Throughput>>((
            Probe::<Wire<FailOnEof>, receive::Gated, _> {
                close: close.clone(),
                retired: done.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Armed(Rc::clone(&wire)),
                marker: PhantomData,
                observe: accepted,
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
                let mut peer = Peer::at(case.addr()).try_read(GUARD).expect("connect peer");
                peer.write_all(b"close before eof observation")
                    .expect("write request");
                assert_eq!(
                    case.try_wait(&wire.chunk, 1).expect("chunk wait"),
                    Wait::Reached,
                    "chunk event count: {}",
                    events.len()
                );
                peer.shutdown(Shutdown::Write).expect("peer half-close");
                assert_eq!(
                    case.try_wait(&close, 1).expect("close wait"),
                    Wait::Reached,
                    "close event count: {}",
                    events.len()
                );
                assert_eq!(
                    case.try_wait(&done, 1).expect("retired wait"),
                    Wait::Reached,
                    "retired event count: {}",
                    events.len()
                );
                assert!(Peer::try_read_all(&peer).expect("peer response").is_empty());
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
    assert_eq!(wire.eof_count(), 1);
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"close before eof observation".to_vec()),
                Event::Timeout,
                Event::Wire(Fault::Truncated, None, 0),
                Event::Close,
                Event::Retired,
            ]
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
