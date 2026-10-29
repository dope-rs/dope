use crate::common::manifold::client::error;

pub mod common;

use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    fmt::Debug,
    io::{Read as _, Write as _},
    rc::Rc,
    time::Duration,
};

use dope_core::{
    dope_sys::io::socket::record::Record,
    driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
    platform::channel::Channel,
};
use dope_manifold::app::env::Bundle;
use dope_net::{link::egress, tcp::Tcp};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers,
    harness::{
        observe::Trace,
        peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::buffer::storage::{self, Shared};

use crate::common::{
    contract::Report,
    manifold::client::send::{
        lane::{self, Signals},
        profile,
        requests::{self, Requests},
        wire::Wire,
    },
    scenario::case::{Connector, Value},
};

#[test]
fn release_wakes_request() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<2>(
        |peers| {
            for peer in peers.iter_mut() {
                peer.write_all(b"open").expect("establish parked wire");
            }
            for peer in peers.iter_mut() {
                let mut byte = [0; 1];
                peer.read_exact(&mut byte).expect("read request");
                assert_eq!(byte, *b"x");
            }
        },
        |address, release| {
            let conn = Gate::new();
            let moved = Gate::new();
            let sent = Gate::new();

            let audit = Rc::new(Report::default());

            let planned =
                Connector::<2>::new(address, Duration::from_secs(1), Duration::from_secs(1))
                    .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
                    .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Slow>>(Value::new(
                        lane::Lane {
                            create: || {
                                let value = Shared::try_copy(b"x").expect("shared request payload");
                                egress::Load::new(value)
                                    .try_nonempty()
                                    .expect("nonempty request payload")
                            },
                            audit: audit.clone(),

                            pending: [const { Cell::new(None) }; 3],
                            signals: Signals {
                                conn: conn.clone(),
                                moved: moved.clone(),
                                sent: sent.clone(),
                                closed: Gate::new(),
                            },
                            abort: Cell::new(false),
                        },
                    ))
                    .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&conn, 2).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            case.try_wait(&moved, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(moved.hits(), 1);
                        release.try_fire().expect("release scripted peers");
                        assert_eq!(
                            case.try_wait(&moved, 2).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            case.try_wait(&sent, 2).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                    },
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            checked(&audit);
            assert_eq!(moved.hits(), 2);
            assert_eq!(sent.hits(), 2);
        },
    )
    .expect("gated peers");
}

#[test]
fn close_relays_capacity() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<3>(
        |peers| {
            for peer in peers.iter_mut() {
                if let Err(error) = peer.write_all(b"open") {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        ),
                        "unexpected open failure: {error}"
                    );
                }
            }
            let mut count = 0;
            for peer in peers.iter_mut() {
                let mut byte = [0; 1];
                match peer.read_exact(&mut byte) {
                    Ok(()) => {
                        assert_eq!(byte, *b"x");
                        count += 1;
                    }
                    Err(error) => {
                        assert!(
                            matches!(
                                error.kind(),
                                std::io::ErrorKind::UnexpectedEof
                                    | std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::BrokenPipe
                            ),
                            "unexpected read failure: {error}"
                        );
                    }
                }
            }
            assert_eq!(count, 2);
        },
        |address, release| {
            let conn = Gate::new();
            let moved = Gate::new();
            let sent = Gate::new();
            let closed = Gate::new();

            let audit = Rc::new(Report::default());

            let planned =
                Connector::<3>::new(address, Duration::from_secs(1), Duration::from_secs(1))
                    .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
                    .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Slow>>(Value::new(
                        lane::Lane {
                            create: || {
                                let value = Shared::try_copy(b"x").expect("shared request payload");
                                egress::Load::new(value)
                                    .try_nonempty()
                                    .expect("nonempty request payload")
                            },
                            audit: audit.clone(),

                            pending: [const { Cell::new(None) }; 3],
                            signals: Signals {
                                conn: conn.clone(),
                                moved: moved.clone(),
                                sent: sent.clone(),
                                closed: closed.clone(),
                            },
                            abort: Cell::new(true),
                        },
                    ))
                    .expect("prepare actual client layout");
            let mut reclaim2 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&conn, 3).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(moved.hits(), 1);
                        release.try_fire().expect("release scripted peers");
                        assert_eq!(
                            case.try_wait(&moved, 2).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            case.try_wait(&sent, 2).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                    },
                    recovery::Policy::new(|event| reclaim2.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert!(
                matches!(reclaim2.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim2:?}"
            );

            checked(&audit);
            assert_eq!(moved.hits(), 2);
            assert_eq!(sent.hits(), 2);
        },
    )
    .expect("gated peers");
}

#[test]
fn credit_grants_source() {
    let mut finish = Completion::new();
    let (steps, script) = Channel::pair().expect("peer command channel");
    peer::TcpScript::with_many::<4, _>(
        move |peers| {
            loop {
                let index = match script.recv() {
                    Record::Value(index) => index as usize,
                    Record::Closed => return Ok(()),
                    Record::Failed(error) => return Err(error),
                };
                let peer = &mut peers[index];
                assert!(peer.write_all(b"open").is_ok(), "establish parked wire");
                let mut marker = [0; 1];
                assert!(peer.read_exact(&mut marker).is_ok(), "read marker");
                assert!(matches!(marker[0], b'5' | b'8'), "invalid request marker");
                let length = if marker[0] == b'5' { 5 } else { 8 };
                let mut suffix = vec![0; length - 1];
                assert!(peer.read_exact(&mut suffix).is_ok(), "read request");
                assert!(suffix.iter().all(|byte| *byte == marker[0]));
            }
        },
        |server| {
            let conn = Gate::new();
            let moved = Gate::new();
            let order = Rc::new(Trace::new());
            let admitted = Rc::new(Trace::new());
            let next = Rc::new(Cell::new(0));
            let sent = Gate::new();

            let audit = Rc::new(Report::default());

            let planned = Connector::<4>::new(
                server.addr(),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .egress(egress::Config::shared(egress::Credit::fixed::<3, 10>()))
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Slow>>(Value::new(requests::Lane {
                audit: audit.clone(),

                requests: Requests {
                    create: |length: Option<usize>| {
                        let length = length.expect("each connection receives one request");
                        assert!(matches!(length, 5 | 8), "unsupported test request length");
                        let marker = if length == 5 { b'5' } else { b'8' };
                        let value = storage::Shared::try_from(&mut vec![marker; length])
                            .expect("request storage freeze");
                        let value = egress::Load::new(value)
                            .try_nonempty()
                            .expect("assigned request is nonempty");
                        (length, value)
                    },
                    pending: RefCell::default(),
                    next: next.clone(),
                    order: order.clone(),
                    admitted: admitted.clone(),
                },
                conn: conn.clone(),
                moved: moved.clone(),
                sent: sent.clone(),
            }))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&conn, 4).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            case.try_wait(&moved, 2).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            &*order.values().expect("request order"),
                            &[5, 5],
                            "resident credit admits exactly two 5-byte requests"
                        );

                        let first = admitted.values().expect("admitted requests")[0];
                        let second = admitted.values().expect("admitted requests")[1];
                        steps
                            .send(u32::try_from(first).expect("peer index"))
                            .expect("drain first request");

                        assert_eq!(
                            case.try_wait(&moved, 3).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            &*order.values().expect("request order"),
                            &[5, 5, 5],
                            "one freed 5 cannot admit the 8-byte request"
                        );
                        let third = admitted.values().expect("admitted requests")[2];
                        for index in [second, third] {
                            steps
                                .send(u32::try_from(index).expect("peer index"))
                                .expect("drain request");
                        }

                        assert_eq!(
                            case.try_wait(&moved, 4).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            &*order.values().expect("request order"),
                            &[5, 5, 5, 8],
                            "the 8-byte request is granted last"
                        );
                        let fourth = admitted.values().expect("admitted requests")[3];
                        steps
                            .send(u32::try_from(fourth).expect("peer index"))
                            .expect("drain fourth request");
                        assert_eq!(
                            case.try_wait(&sent, 4).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                    },
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            checked(&audit);
            assert_eq!(audit.cancelled.get(), Some(0));
            assert_eq!(next.get(), 4);
            assert_eq!(moved.hits(), 4);
            assert_eq!(sent.hits(), 4);
            assert_eq!(admitted.values().expect("admitted requests").len(), 4);

            drop(steps);
            server.finish().expect("gated peers").expect("peer command");
        },
    )
    .expect("spawn scripted peers");
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}

fn checked(audit: &Report<error::Error, 16>) {
    let faults = audit.faults.values().expect("fault observations");
    let result = error::Error::check(&faults);
    assert!(result.is_ok(), "{result:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
