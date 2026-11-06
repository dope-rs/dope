use crate::common::manifold::client::error;

pub mod common;

use std::{cell::Cell, fmt::Debug, net::Ipv4Addr, rc::Rc, time::Duration};

use dope_core::driver::{
    life::recovery,
    route::{Id, KeyTag},
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::session::{Setup, ingress},
    network::client::topology::plan::{Plan, Spec},
    service::{Endpoint, Revision, Snapshot, health::Domain, sync::Preserve},
};
use dope_net::{link::egress, tcp::Tcp, wire::domain::Local};
use dope_runtime::{
    executor::session::{Completion, Scope},
    topology::error::Kind,
};
use dope_test::{
    fibers,
    harness::{
        GUARD,
        observe::Trace,
        peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::depot::{
    array::{self, Len},
    queue::pool::fair,
};

use crate::common::{
    contract::Report,
    manifold::client::send::{
        lane::{self, Signals},
        profile,
        requests::{self, Requests},
        wire::Wire,
    },
    recover,
    scenario::{
        Runtime,
        case::{Connector, Value},
    },
};

#[test]
fn release_wakes_request() {
    let mut finish = Completion::new();
    let acceptor = peer::TcpAcceptor::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("bind peers");
    let address = acceptor.local_addr().expect("peer address");
    let mut peers: Option<[peer::Stream<'static>; 2]> = None;
    let signals = Signals::default();
    let audit = Rc::new(Report::default());
    let planned = Connector::<2>::new(address, Duration::from_secs(1), Duration::from_secs(1))
        .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Slow>>(Value::new(lane::Lane {
            audit: audit.clone(),
            pending: [const { Cell::new(None) }; 3],
            signals: signals.clone(),
            abort: Cell::new(false),
        }))
        .expect("prepare actual client layout");
    let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            |case| {
                assert_eq!(
                    case.try_wait(&signals.conn, 2).expect("progress wait"),
                    fibers::Wait::Reached
                );
                assert_eq!(
                    case.try_wait(&signals.moved, 1).expect("progress wait"),
                    fibers::Wait::Reached
                );
                assert_eq!(signals.moved.hits(), 1);
                let peers = peers.insert(std::array::from_fn(|_| {
                    acceptor
                        .accept_for(GUARD)
                        .expect("accept peer")
                        .expect("connected peer")
                }));
                for peer in peers.iter() {
                    peer.set_nodelay(true).expect("TCP_NODELAY");
                    peer.read_wait(Some(GUARD)).expect("read deadline");
                    peer.write_wait(Some(GUARD)).expect("write deadline");
                    peer.write_all(b"open").expect("establish parked wire");
                }
                assert_eq!(
                    case.try_wait(&signals.moved, 2).expect("progress wait"),
                    fibers::Wait::Reached
                );
                assert_eq!(
                    case.try_wait(&signals.sent, 2).expect("progress wait"),
                    fibers::Wait::Reached
                );
                for peer in peers.iter() {
                    let mut byte = [0; 1];
                    peer.read_exact(&mut byte).expect("read request");
                    assert_eq!(byte, *b"x");
                }
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
    assert_eq!(signals.moved.hits(), 2);
    assert_eq!(signals.sent.hits(), 2);
}

#[test]
fn close_relays_capacity() {
    let mut finish = Completion::new();
    let acceptor = peer::TcpAcceptor::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("bind peers");
    let address = acceptor.local_addr().expect("peer address");
    let mut peers: Option<[peer::Stream<'static>; 3]> = None;
    let signals = Signals::default();
    let audit = Rc::new(Report::default());
    let planned = Connector::<3>::new(address, Duration::from_secs(1), Duration::from_secs(1))
        .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Slow>>(Value::new(lane::Lane {
            audit: audit.clone(),
            pending: [const { Cell::new(None) }; 3],
            signals: signals.clone(),
            abort: Cell::new(true),
        }))
        .expect("prepare actual client layout");
    let mut reclaim2 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            |case| {
                assert_eq!(
                    case.try_wait(&signals.conn, 3).expect("progress wait"),
                    fibers::Wait::Reached
                );
                assert_eq!(
                    case.try_wait(&signals.closed, 1).expect("progress wait"),
                    fibers::Wait::Reached
                );
                assert_eq!(signals.moved.hits(), 1);
                let peers = peers.insert(std::array::from_fn(|_| {
                    acceptor
                        .accept_for(GUARD)
                        .expect("accept peer")
                        .expect("connected peer")
                }));
                for peer in peers.iter() {
                    peer.set_nodelay(true).expect("TCP_NODELAY");
                    peer.read_wait(Some(GUARD)).expect("read deadline");
                    peer.write_wait(Some(GUARD)).expect("write deadline");
                    if let Err(error) = peer.write_all(b"open") {
                        assert!(
                            matches!(error, peer::Error::Syscall(error) if error.is_reset() || error.is_pipe()),
                            "unexpected open failure: {error:?}"
                        );
                    }
                }
                assert_eq!(
                    case.try_wait(&signals.moved, 2).expect("progress wait"),
                    fibers::Wait::Reached
                );
                assert_eq!(
                    case.try_wait(&signals.sent, 2).expect("progress wait"),
                    fibers::Wait::Reached
                );
                let mut count = 0;
                for peer in peers.iter() {
                    let mut byte = [0; 1];
                    match peer.read_exact(&mut byte) {
                        Ok(()) => {
                            assert_eq!(byte, *b"x");
                            count += 1;
                        }
                        Err(error) => {
                            assert!(
                                matches!(error, peer::Error::UnexpectedEof)
                                    || matches!(error, peer::Error::Syscall(error) if error.is_reset() || error.is_pipe()),
                                "unexpected read failure: {error:?}"
                            );
                        }
                    }
                }
                assert_eq!(count, 2);
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
    assert_eq!(signals.moved.hits(), 2);
    assert_eq!(signals.sent.hits(), 2);
}

#[test]
fn credit_grants_source() {
    let mut finish = Completion::new();
    let acceptors: [_; 4] = std::array::from_fn(|_| {
        peer::TcpAcceptor::bind((Ipv4Addr::LOCALHOST, 0).into()).expect("bind peer")
    });
    let addresses = acceptors
        .each_ref()
        .map(|acceptor| acceptor.local_addr().expect("peer address"));
    let mut peers: Option<[peer::Stream<'static>; 4]> = None;
    let conn = Gate::new();
    let moved = Gate::new();
    let order = Rc::new(Trace::new());
    let admitted = Rc::new(Trace::new());
    let next = Rc::new(Cell::new(0));
    let sent = Gate::new();
    let audit = Rc::new(Report::default());
    let plan = Plan::<KeyTag<0>, 4>::new(
        4,
        Spec::new(Duration::from_secs(1), Duration::from_secs(1))
            .expect("retry bounds")
            .with_egress(egress::Config::shared(egress::Credit::fixed::<3, 10>())),
    )
    .expect("client layout");
    let runtime =
        Runtime::try_for_plan::<profile::Slow, _>(&plan).expect("prepare actual client layout");
    let mut reclaim1 = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

    runtime.with_scope(
        |mut runtime| {
            let setup = Setup::new(
                requests::Lane {
                    audit: audit.clone(),
                    requests: Requests {
                        pending: fair::Lanes::try_new(
                            fair::Plan::new(4, 4).expect("request lanes"),
                            &runtime.driver().regions().0,
                        )
                        .expect("source storage"),
                        assigned: Default::default(),
                        peers: [None; 4],
                        next: next.clone(),
                        order: order.clone(),
                        admitted: admitted.clone(),
                    },
                    conn: conn.clone(),
                    moved: moved.clone(),
                    sent: sent.clone(),
                },
                ingress::Plan::none(),
            );
            let snapshot = Snapshot::from_vec(
                Revision::new(1),
                array::Vec::from_fn(Len::exact::<4>(), |slot| {
                    let address = addresses[slot.index()];
                    Endpoint::new(address, address)
                }),
            );
            let hash = runtime.hash_state(Domain::for_id(0));
            plan.bind(Local::<Wire, KeyTag<0>>::new(()), hash)
                .install::<_, _, Preserve, _, Bundle<Tcp, Wire, profile::Slow>, 4, _, _>(
                    setup,
                    snapshot,
                    &mut runtime,
                    Scope::driver,
                    |pool, runtime| {
                        runtime.with_quiesce(
                            &mut finish,
                            Host::new(pool),
                            Duration::from_secs(2),
                            |mut app| {
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &conn, 4)
                                        .expect("progress wait"),
                                    fibers::Wait::Reached
                                );
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &moved, 2)
                                        .expect("progress wait"),
                                    fibers::Wait::Reached
                                );
                                assert_eq!(
                                    &*order.values().expect("request order"),
                                    &[5, 5],
                                    "resident credit admits exactly two 5-byte requests"
                                );
                                let peers = peers.insert(std::array::from_fn(|index| {
                                    acceptors[index]
                                        .accept_for(GUARD)
                                        .expect("accept peer")
                                        .expect("connected peer")
                                }));
                                for peer in peers.iter() {
                                    peer.set_nodelay(true).expect("TCP_NODELAY");
                                    peer.read_wait(Some(GUARD)).expect("read deadline");
                                    peer.write_wait(Some(GUARD)).expect("write deadline");
                                }
                                let peer = |address| {
                                    &peers[addresses
                                        .iter()
                                        .position(|expected| *expected == address)
                                        .expect("connected endpoint")]
                                };
                                let first = admitted.values().expect("admitted requests")[0];
                                let second = admitted.values().expect("admitted requests")[1];
                                peer(first)
                                    .write_all(b"open")
                                    .expect("establish parked wire");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &moved, 3)
                                        .expect("progress wait"),
                                    fibers::Wait::Reached
                                );
                                assert_eq!(
                                    &*order.values().expect("request order"),
                                    &[5, 5, 5],
                                    "one freed 5 cannot admit the 8-byte request"
                                );
                                let third = admitted.values().expect("admitted requests")[2];
                                for address in [second, third] {
                                    peer(address)
                                        .write_all(b"open")
                                        .expect("establish parked wire");
                                }
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &moved, 4)
                                        .expect("progress wait"),
                                    fibers::Wait::Reached
                                );
                                assert_eq!(
                                    &*order.values().expect("request order"),
                                    &[5, 5, 5, 8],
                                    "the 8-byte request is granted last"
                                );
                                let fourth = admitted.values().expect("admitted requests")[3];
                                peer(fourth)
                                    .write_all(b"open")
                                    .expect("establish parked wire");
                                assert_eq!(
                                    fibers::TEST
                                        .try_wait(&mut app, &sent, 4)
                                        .expect("progress wait"),
                                    fibers::Wait::Reached
                                );
                                for (&address, &length) in admitted
                                    .values()
                                    .expect("admitted requests")
                                    .iter()
                                    .zip(order.values().expect("request order").iter())
                                {
                                    let mut bytes = [0; 8];
                                    let bytes = &mut bytes[..length];
                                    peer(address).read_exact(bytes).expect("read request");
                                    let marker = if length == 5 { b'5' } else { b'8' };
                                    assert!(bytes.iter().all(|byte| *byte == marker));
                                }
                            },
                        )
                    },
                )
                .expect("install client")
                .expect("client teardown");
        },
        recovery::Policy::new(|event| reclaim1.push(event), recover),
    );
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
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}

fn checked(audit: &Report<error::Error, 16>) {
    let faults = audit.faults.values().expect("fault observations");
    assert!(faults.is_empty(), "{faults:?}");
}
