pub mod common;

use std::{
    cell::Cell, convert::Infallible, io::Write as _, net::TcpStream, rc::Rc, time::Duration,
};

use common::manifold::listener::{native, reached};
use common::{
    manifold::{
        Pattern,
        listener::{
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
        life::{creation::Attempt, recovery},
        route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
        schedule,
    },
    io::{
        Reclaimed,
        event::{batch::Flow, net::accept::Failure},
        socket::Backlog,
    },
    platform::wake,
};
use dope_manifold::{
    app::{Host, env::Bundle},
    network::listener::{self, Domain, config},
    timing::Throughput,
};
use dope_net::{
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::executor::{
    self, drive,
    session::{self, Completion},
};
use dope_test::{
    fibers::{self, Wait},
    harness::{
        GUARD,
        peer::Peer,
        server::Pair,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn forced_detaches_credit() {
    let (received, retained) = wake::Ends::blocking()
        .expect("retained receive signal")
        .split();
    let harness = Pair::bind().expect("bind retained-credit harness");
    let bind = harness.addr();
    let result = harness.run(
        move |source, bound| {
            let cfg = config::Prepared::<Tcp> {
                connections: Capacity::fixed::<8>(),
                per_ip: None,
                direct_flights: 0.into(),
                bind,
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
            let mut create = Attempt::new();
            let executor = executor::Build::try_build(source, &mut create, driver);
    executor.as_ref().map_err(|error| error.kind()).expect("create executor");
    let executor = executor.ok().expect("executor");
            let mut reports = Reports::<_>::new(driver, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
            executor.run(|executor| {
                let result = executor.enter_with(
                    |mut session| {
                        let hash = session.hash_state(Domain::DEFAULT);
                        let result = listener::Listener::<KeyTag<0>, App<'_, '_, _>, Bundle<Tcp, Teardown, Throughput>>::adopt_in(
                            App {
                                held: None,
                                observed: move || retained.notify().expect("retained receive signal"),
                            }, cfg, bound, hash, &mut session,
                            session::Scope::driver,
                            |listener, session| {
                                let result = session.run_quiesce(
                                    &mut finish, Host::new(listener), Duration::ZERO,
                                );
                                let failure = result.err().expect("zero-duration quiescence must time out after forced cleanup");
                                assert!(matches!(
                                    &failure,
                                    session::Error::Quiesce {
                                        output: Ok(()),
                                        drive: drive::Error::QuiescenceTimeout,
                                        disarm: None,
                                    }
                                ), "quiescence must preserve completed shutdown and its timeout: {failure:?}");
                                session.with_quiesce(
                                    &mut finish, Empty {}, Duration::from_secs(2), |_| {},
                                ).expect("drain retained-credit executor");
                            },
                        );
                        assert!(result.is_ok(), "adopt retained-credit listener");
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                );
                assert_eq!(reports.events().expect("reclaimed events").len(), pending, "each pending accept retires once");
                for report in reports.into_events().expect("reclaimed events") {
                    let error = match report {
                        Reclaimed::Accept { result: Err(error), flow: Flow::Last, .. } => Some(error),
                        _ => None,
                    }.expect("each forced accept retires with a terminal error");
                    assert!(matches!(error, Failure::Syscall(errno) if errno.is_canceled()),
                        "unexpected forced retirement: {error:?}");
                }
                result.as_ref().map_err(|error| error.cause()).expect("finish executor");
            });
            Ok::<(), Infallible>(())
        },
        |addr| {
            let mut stream = TcpStream::connect(addr).expect("connect retained-credit peer");
            stream.write_all(b"held receive credit").expect("write retained-credit request");
            received.wait_timeout(GUARD).expect("retained receive signal");
            Some(stream)
        },
    );
    assert!(
        result.is_ok(),
        "forced shutdown must release every retained credit"
    );
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
                    |listener, session| {
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
                    |listener, session| {
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
