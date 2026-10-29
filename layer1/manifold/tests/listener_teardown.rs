pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    env,
    fmt::Debug,
    io::{self, Write as _},
    net::TcpStream,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use common::{
    manifold::{
        Pattern,
        listener::{
            finish::{
                close::host,
                drain::{Drain, Event},
                teardown::{App, Teardown, try_config},
            },
            trace::One,
        },
    },
    scenario::{app::Empty, case::listener::Listener},
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
};
use dope_manifold::{
    app::{Host, env::Bundle},
    network::listener::{self, Domain, config, conn::write},
    timing::Throughput,
};
use dope_net::{
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::{
    executor::{
        self, Executor, drive,
        session::{self, Completion},
    },
    topology::error::Kind,
};
use dope_test::{
    fibers::{self, Run, Wait},
    harness::{
        GUARD,
        peer::Peer,
        server::Pair,
        signals::{Gate, Progress as _},
        support::file::Directory,
    },
    reclaim::Reports,
};

fn written(result: io::Result<()>) {
    result.expect("write retained receive marker");
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

fn drain(event: Event<'_>) {
    match event {
        Event::Chunk(len) => assert_ne!(len, 0),
        Event::Write(result) => assert!(matches!(result, Ok(write::Outcome::Committed(())))),
    }
}

#[test]
fn forced_detaches_credit() {
    let base = env::temp_dir();
    let dir = Directory::try_with(&base, "listener_retained_credit", |result| {
        result.expect("fixture cleanup");
    })
    .expect("temporary path");
    let marker = dir.path().join("held.marker");
    let harness = Pair::bind().expect("bind retained-credit harness");
    let bind = harness.addr();
    let server = marker.clone();
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
                            App { held: None, marker: server, observed: written }, cfg, bound, hash, &mut session,
                            session::Scope::driver,
                            |listener, session| {
                                let result = session.run_quiesce(
                                    &mut finish, Host::new(listener), Duration::ZERO,
                                );
                                assert!(result.is_err(), "zero-duration quiescence must time out after forced cleanup");
                                let Err(failure) = result else { return; };
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
            let start = Instant::now();
            while !marker.exists() {
                assert!(start.elapsed() < GUARD, "retained receive marker was not written");
                thread::yield_now();
            }
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
                observe: drain,
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
                observe: drain,
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
    let mut attempt = Attempt::new();
    let config = One::driver().expect("settings");
    let mut reports = Reports::new(config, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let executor = Executor::new(&mut attempt, config);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("create executor");
    let executor = executor.ok().expect("executor");
    executor.run(|executor| {
        let result = executor.enter_with(
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
                                    host::Host { listener, id },
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
        assert!(result.is_ok());
    });
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
