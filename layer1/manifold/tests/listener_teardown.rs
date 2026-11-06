pub mod common;

use std::{cell::Cell, net::SocketAddr, rc::Rc, time::Duration};

use common::manifold::listener::{native, reached};
use common::{
    manifold::{
        Pattern,
        listener::{
            coupled::QUEUED_BODY,
            egress::{App as Egress, Event},
            finish::{
                close::host,
                drain::Drain,
                teardown::{App, Teardown, try_config},
            },
            trace::One,
        },
    },
    recover,
    scenario::{Runtime, app::Empty, case::listener::Listener},
};
use dope_core::{
    Backend,
    dope_sys::io::socket::accept::Source,
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
        schedule,
    },
    io::{
        Reclaimed,
        event::{batch::Flow, net::accept::Failure},
        socket::Backlog,
    },
};
use dope_manifold::{
    app::{Host, env::Bundle},
    network::listener::{self, Domain, config, handler},
    timing::Throughput,
};
use dope_net::{
    link::egress,
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::executor::{
    drive,
    session::{self, Completion},
};
use dope_test::{
    fibers::{self, Wait},
    harness::{
        GUARD,
        observe::Trace,
        peer::Peer,
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn forced_releases_direct_owner() {
    let cfg = config::Prepared::<Tcp> {
        connections: Capacity::fixed::<4>(),
        per_ip: None,
        direct_flights: 1.into(),
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        backlog: Backlog::fixed::<16>(),
        stream: dope_net::tcp::StreamConfig {
            send_buffer_size: Some(
                dope_core::io::socket::option::Buffer::try_from(1024_usize).expect("socket buffer"),
            ),
            ..Default::default()
        },
        transport: Default::default(),
        egress: egress::Config::shared(egress::Credit::fixed::<0, 0>()),
    };
    let driver = try_config(cfg.connections()).expect("listener settings");
    let mut finish = Completion::new();
    let admitted = Gate::new();
    let released = Gate::new();
    let returned = Gate::new();
    let events = Rc::new(Trace::new());
    Runtime::new(driver).with_scope(
        |mut session| {
            let hash = session.hash_state(Domain::DEFAULT);
            listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                cfg,
                domain::Local::new(Identity),
            )
            .with_driver(
                Egress {
                    response: &QUEUED_BODY,
                    admitted: admitted.clone(),
                    closed: Gate::new(),
                    released: released.clone(),
                    returned: returned.clone(),
                    retired: Gate::new(),
                    abort: false,
                    events: Rc::clone(&events),
                },
                hash,
                &mut session,
                session::Scope::driver,
                |listener, _wait, session| {
                    let peer = Peer::at(listener.local_addr())
                        .try_connect()
                        .expect("connect peer");
                    peer.write_all(b"request").expect("write request");
                    let result = session.with_quiesce(
                        &mut finish,
                        Host::new(listener),
                        Duration::ZERO,
                        |mut app| {
                            reached(
                                fibers::TEST.try_wait(&mut app, &admitted, 1),
                                admitted.hits(),
                                1,
                            );
                            assert_eq!((returned.hits(), released.hits()), (0, 0));
                        },
                    );
                    assert!(matches!(
                        result,
                        Err(session::Error::Quiesce {
                            output: (),
                            drive: drive::Error::QuiescenceTimeout,
                            disarm: None,
                        })
                    ));
                    drop(peer);
                },
            )
            .expect("install listener");
        },
        recovery::Policy::new(|_| {}, recover),
    );
    assert_eq!((returned.hits(), released.hits()), (1, 1));
    assert!(matches!(&*events.values().expect("release events"), [
        Event::Chunk(_, _),
        Event::Admitted(_, _, _, _),
        Event::Released(_, body, 0, handler::Disposition::Closed),
    ] if body == &QUEUED_BODY));
}

#[test]
fn forced_detaches_credit() {
    let cfg = config::Prepared::<Tcp> {
        connections: Capacity::fixed::<8>(),
        per_ip: None,
        direct_flights: 0.into(),
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        backlog: Backlog::fixed::<16>(),
        stream: Default::default(),
        transport: Default::default(),
        egress: Default::default(),
    };
    let pending = (cfg.connections().nonzero().get() - 1)
        .min(<Backend as Source>::MAX_IN_FLIGHT.get())
        .min(schedule::LIMIT);
    let driver = try_config(cfg.connections()).expect("retained-credit listener settings");
    let mut finish = Completion::new();
    let retained = LocalGate::new();
    let retained = retained.handle();
    let mut reports = Reports::<_>::new(driver, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    Runtime::new(driver).with_scope(
        |mut session| {
            let hash = session.hash_state(Domain::DEFAULT);
            let result = listener::Install::<KeyTag<0>, Bundle<Tcp, Teardown, Throughput>, _>::new(
                cfg,
                domain::Local::new(()),
            )
            .with_driver(
                App {
                    held: None,
                    observed: move || retained.hit(),
                },
                hash,
                &mut session,
                session::Scope::driver,
                |listener, _wait, session| {
                    let peer = Peer::at(listener.local_addr())
                        .try_connect()
                        .expect("connect retained-credit peer");
                    peer.write_wait(Some(GUARD)).expect("peer write timeout");
                    peer.write_all(b"held receive credit")
                        .expect("write retained-credit request");
                    let result = session.with_quiesce(
                        &mut finish,
                        Host::new(listener),
                        Duration::ZERO,
                        |mut app| {
                            reached(
                                fibers::TEST.try_wait(&mut app, &retained, 1),
                                retained.hits(),
                                1,
                            );
                            Ok::<(), drive::Error>(())
                        },
                    );
                    let failure = result
                        .expect_err("zero-duration quiescence must time out after forced cleanup");
                    assert!(
                        matches!(
                            &failure,
                            session::Error::Quiesce {
                                output: Ok(()),
                                drive: drive::Error::QuiescenceTimeout,
                                disarm: None,
                            }
                        ),
                        "quiescence must preserve completed shutdown and its timeout: {failure:?}"
                    );
                    session
                        .with_quiesce(&mut finish, Empty {}, Duration::from_secs(2), |_| {})
                        .expect("drain retained-credit executor");
                    drop(peer);
                },
            );
            assert!(result.is_ok(), "install retained-credit listener");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    let mut accepts = 0;
    for report in reports.into_events().expect("reclaimed events") {
        let error = match report {
            Reclaimed::Accept {
                result: Err(Failure::Syscall(error)),
                flow: Flow::Last,
                ..
            } => {
                accepts += 1;
                error
            }
            Reclaimed::Recv {
                result: Err(error),
                flow: Flow::Last,
                ..
            } => error,
            _ => panic!("unexpected forced retirement: {report:?}"),
        };
        assert!(
            error.is_canceled(),
            "unexpected forced retirement: {error:?}"
        );
    }
    assert_eq!(accepts, pending, "each pending accept retires once");
}

#[test]
fn shutdown_scans_capacity() {
    let mut finish = Completion::new();
    let prepared = Listener::new::<4096>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Drain {
                payload: Vec::new(),
                gate: Gate::new(),
            },
            Identity,
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| assert!(case.addr().ip().is_loopback()),
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reply_drains_first() {
    let mut finish = Completion::new();
    let want = Pattern(12_000).bytes();
    let gate = Gate::new();
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Drain {
                payload: want.clone(),
                gate: gate.clone(),
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
                    .exchange(b"GET\n".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        let got = peer
                            .finish()
                            .expect("finish client peer")
                            .expect("client peer failed");

                        assert_eq!(got, want, "reply truncated or corrupted on the drain path");
                        assert_eq!(gate.hits(), 1, "connection must close exactly once");
                    })
                    .expect("peer spawn failed");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn control_closes_idle() {
    let config = One::driver().expect("settings");
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    Runtime::new(config).with_scope(
        |mut session| {
            let mut finish = Completion::new();
            let closed = Gate::new();
            let id = Rc::new(Cell::new(None));
            let hash = session.hash_state(listener::Domain::DEFAULT);
            let install =
                listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                    One::config(),
                    domain::Local::new(Identity),
                )
                .with_driver(
                    host::App {
                        id: Rc::clone(&id),
                        accepted: Gate::new(),
                        received: Gate::new(),
                        closed: closed.clone(),
                    },
                    hash,
                    &mut session,
                    session::Scope::driver,
                    |listener, _wait, session| {
                        let addr = listener.local_addr();
                        session
                            .with_quiesce(
                                &mut finish,
                                host::Host {
                                    listener,
                                    id,
                                    checked: None,
                                },
                                Duration::from_secs(2),
                                |mut app| {
                                    let peer = Peer::at(addr).try_connect().expect("connect");
                                    reached(
                                        fibers::TEST.try_wait(&mut app, &closed, 1),
                                        closed.hits(),
                                        1,
                                    );
                                    drop(peer);
                                },
                            )
                            .expect("close idle peer");
                    },
                );
            assert!(install.is_ok());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reinstalls_reject_stale_id() {
    let config = One::driver().expect("settings");
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    Runtime::new(config).with_scope(
        |mut session| {
            let mut finish = Completion::new();
            let mut previous = None;
            for _ in 0..2 {
                let accepted = Gate::new();
                let received = Gate::new();
                let closed = Gate::new();
                let observed = Rc::new(Cell::new(None));
                let requested = Rc::new(Cell::new(None));
                let matched = Rc::new(Cell::new(false));
                let checked = Gate::new();
                let hash = session.hash_state(listener::Domain::DEFAULT);
                listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                    One::config(),
                    domain::Local::new(Identity),
                )
                .with_driver(
                    host::App {
                        id: Rc::clone(&observed),
                        accepted: accepted.clone(),
                        received: received.clone(),
                        closed: closed.clone(),
                    },
                    hash,
                    &mut session,
                    session::Scope::driver,
                    |listener, _wait, session| {
                        let addr = listener.local_addr();
                        session
                            .with_quiesce(
                                &mut finish,
                                host::Host {
                                    listener,
                                    id: Rc::clone(&requested),
                                    checked: Some((Rc::clone(&matched), checked.clone())),
                                },
                                Duration::from_secs(2),
                                |mut app| {
                                    let peer = Peer::at(addr).try_connect().expect("connect");
                                    assert_eq!(
                                        fibers::TEST
                                            .try_wait(&mut app, &accepted, 1)
                                            .expect("accept"),
                                        Wait::Reached,
                                    );
                                    let current = observed.get().expect("accepted id");
                                    if let Some(old) = previous {
                                        requested.set(Some(old));
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &checked, 1)
                                                .expect("query old id"),
                                            Wait::Reached,
                                        );
                                        assert!(
                                            !matched.get(),
                                            "previous installation id accessed new connection"
                                        );
                                        peer.write_all(b"x").expect("write after stale close");
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &received, 1)
                                                .expect("receive after stale close"),
                                            Wait::Reached,
                                        );
                                        assert_eq!(
                                            closed.hits(),
                                            0,
                                            "old id closed new connection"
                                        );
                                        requested.set(Some(current));
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(&mut app, &closed, 1)
                                                .expect("close current id"),
                                            Wait::Reached,
                                        );
                                        assert!(
                                            matched.get(),
                                            "current id must select its connection"
                                        );
                                    } else {
                                        previous = Some(current);
                                    }
                                    Peer::try_reset(peer).expect("reset peer");
                                },
                            )
                            .expect("retire listener");
                    },
                )
                .expect("install same route");
            }
        },
        recovery::Policy::new(|event| reports.push(event), recover),
    );
    assert!(reports.events().expect("reclaimed events").is_empty());
}
