pub mod common;

use common::manifold::listener::{native, reached};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{
    iter,
    net::Shutdown,
    time::{self, Duration},
};

use common::{
    manifold::listener::defer::{
        app,
        preamble::{self, FRAMES, HANDSHAKE, PREFIX},
        script::{self, BODY_LEN, Payload, Until},
        wire::{Profile, Wire},
    },
    recover,
    scenario::case::listener::Listener,
};
use dope_core::{
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
        settings,
        settings::claim::Needs,
    },
    io::socket::{self, option},
};
use dope_manifold::{
    app::{Host, env::Bundle, receive},
    network::listener::{self, config, conn::write, topology},
    timing::Throughput,
};
use dope_net::{
    link::egress,
    tcp::{StreamConfig, Tcp},
    wire::domain,
};
use dope_runtime::executor::{
    Executor,
    session::{self, Completion},
};
use dope_test::{
    alloc::Tracker,
    fibers,
    harness::{
        GUARD,
        peer::{self, Peer},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

fn predecessor(event: app::Event<'_, '_>) {
    match event {
        app::Event::Chunk(len) => assert_ne!(len, 0),
        app::Event::Write(result) => assert!(matches!(
            result,
            Ok(write::Outcome::Committed(())
                | write::Outcome::Deferred(write::Deferred::OrderedDrain | write::Deferred::Turn))
        )),
    }
}

fn direct(event: script::Event<'_, '_>) {
    match event {
        script::Event::Chunk(len) => assert_ne!(len, 0),
        script::Event::Write(result, ops) => {
            assert_eq!(ops, (0, 0));
            assert!(matches!(
                result,
                Ok(write::Outcome::Committed(())
                    | write::Outcome::Deferred(write::Deferred::Capacity))
            ));
        }
        script::Event::Close {
            state,
            index,
            done,
            deferred,
            closed,
        } => assert!(
            matches!(state, script::State::Done),
            "fresh slot {index} closed: done={done}, deferred={deferred}, closed={closed}",
        ),
    }
}

fn preamble(event: preamble::Event<'_, '_>) {
    match event {
        preamble::Event::Write(result) => result.expect("preamble wire write"),
        preamble::Event::Input { expected, actual } => assert_eq!(actual, expected),
        preamble::Event::Consumed { expected, actual } => assert_eq!(actual, expected),
        preamble::Event::Enqueued(result) => {
            assert!(matches!(result, Ok(write::Outcome::Committed(()))))
        }
        preamble::Event::Chunk(bytes) => assert!(!bytes.is_empty()),
        preamble::Event::Eof(_) => {}
    }
}

#[test]
fn predecessor_wakes() {
    let mut finish = Completion::new();
    let done = Gate::new();
    let ordered = Gate::new();
    let closed = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(1)
        .egress(egress::Config::shared(egress::Credit::fixed::<1, 0>()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            app::App {
                done: done.clone(),
                ordered: ordered.clone(),
                closed: closed.clone(),
                observe: predecessor,
            },
            (),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                let peer = Peer::at(case.addr()).try_read(GUARD).expect("connect peer");
                reached(case.try_wait(&ordered, 1), ordered.hits(), 1);
                assert_eq!(done.hits(), 1);
                peer.write_wait(Some(GUARD)).expect("peer write timeout");
                peer.write_all(b"open").expect("open wire");
                peer.shutdown(Shutdown::Write).expect("peer write shutdown");
                reached(case.try_wait(&done, 2), done.hits(), 2);
                reached(case.try_wait(&closed, 1), closed.hits(), 1);
                assert_eq!(Peer::try_read_all(&peer).expect("peer I/O"), b"AbodyBbody");
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
fn reports_early_frames() {
    let mut finish = session::Completion::new();
    let gate = Gate::new();
    let prepared = Listener::new::<16>(Default::default())
        .direct(1)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, preamble::Preamble<_>, Throughput>>((
            preamble::App {
                frames: FRAMES,
                gate: gate.clone(),
                observe: preamble,
            },
            (preamble, false),
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
                        |stream| {
                            stream.write_all(HANDSHAKE)?;
                            Peer::try_read_all(stream)
                        },
                        |peer| {
                            reached(case.try_wait(&gate, 1), gate.hits(), 1);
                            let got = peer
                                .finish()
                                .expect("finish client peer")
                                .expect("client peer failed");
                            let want: Vec<u8> = FRAMES
                                .iter()
                                .flat_map(|frame| iter::once(PREFIX).chain(frame.iter().copied()))
                                .collect();
                            assert_eq!(got, want);
                        },
                    )
                    .expect("spawn handshake peer");
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
fn recycle_resumes_waiter() {
    let mut finish = Completion::new();
    let setting = option::Buffer::try_from(1024_usize);
    let setting = setting.expect("socket buffer size");
    let done = Gate::new();
    let deferred = Gate::new();
    let closed = Gate::new();
    let prepared = Listener::new::<4>(Default::default())
        .direct(2)
        .stream(StreamConfig {
            send_buffer_size: Some(setting),
            no_delay: Some(option::Switch::On),
            ..Default::default()
        })
        .egress(egress::Config::shared(egress::Credit::fixed::<0, 0>()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            script::App {
                done: done.clone(),
                deferred: deferred.clone(),
                closed: closed.clone(),
                payload: Payload::Body,
                observe: direct,
            },
            (),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            move |case| {
                let peers = [(); 2].map(|()| {
                    let peer = Peer::at(case.addr()).try_connect().expect("connect peer");
                    peer.set_nodelay(true).expect("peer nodelay");
                    peer.write_wait(Some(GUARD)).expect("peer write timeout");
                    peer.write_all(b"open").expect("open wire");
                    peer.shutdown(Shutdown::Write).expect("peer half-close");
                    peer.nonblock(true);
                    peer
                });
                reached(case.try_wait(&done, 2), done.hits(), 2);
                let third = Peer::at(case.addr()).try_connect().expect("third peer");
                third.set_nodelay(true).expect("peer nodelay");
                third.write_wait(Some(GUARD)).expect("peer write timeout");
                third.write_all(b"open").expect("open wire");
                third.shutdown(Shutdown::Write).expect("peer half-close");
                third.nonblock(true);
                reached(case.try_wait(&deferred, 1), deferred.hits(), 1);
                assert_eq!(done.hits(), 2);
                for peer in peers {
                    let started = time::Instant::now();
                    let mut received = 0;
                    let mut bytes = [0; 4096];
                    loop {
                        assert!(started.elapsed() < GUARD, "peer drain timeout");
                        match peer.read(&mut bytes) {
                            Ok(0) => break,
                            Ok(count) => received += count,
                            Err(error) => {
                                assert!(
                                    matches!(error, peer::Error::Syscall(error)
                                        if error.is_block() || error.is_intr()),
                                    "read peer: {error:?}"
                                );
                                case.try_drive(fibers::Yield::new()).expect("drive peer");
                            }
                        }
                    }
                    assert_eq!(received, BODY_LEN + 1);
                }
                reached(case.try_wait(&done, 3), done.hits(), 3);
                reached(case.try_wait(&closed, 2), closed.hits(), 2);
                let fourth = Peer::at(case.addr()).try_connect().expect("fourth peer");
                fourth.set_nodelay(true).expect("peer nodelay");
                fourth.write_wait(Some(GUARD)).expect("peer write timeout");
                fourth.write_all(b"open").expect("open wire");
                fourth.shutdown(Shutdown::Write).expect("peer half-close");
                fourth.nonblock(true);
                reached(case.try_wait(&done, 4), done.hits(), 4);
                assert_eq!(deferred.hits(), 1);
                for peer in [third, fourth] {
                    let started = time::Instant::now();
                    let mut received = 0;
                    let mut bytes = [0; 4096];
                    loop {
                        assert!(started.elapsed() < GUARD, "peer drain timeout");
                        match peer.read(&mut bytes) {
                            Ok(0) => break,
                            Ok(count) => received += count,
                            Err(error) => {
                                assert!(
                                    matches!(error, peer::Error::Syscall(error)
                                        if error.is_block() || error.is_intr()),
                                    "read peer: {error:?}"
                                );
                                case.try_drive(fibers::Yield::new()).expect("drive peer");
                            }
                        }
                    }
                    assert_eq!(received, BODY_LEN + 1);
                }
                reached(case.try_wait(&closed, 4), closed.hits(), 4);
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
fn deadline_resumes_waiter() {
    let mut finish = Completion::new();
    use settings::Resource as _;

    let done = Gate::new();
    let deferred = Gate::new();
    let closed = Gate::new();
    let layout = topology::Layout::<KeyTag<0>, receive::Borrowed>::new(Capacity::fixed::<2>());
    let needs = layout.include_in(Needs::new());
    let needs = needs.expect("listener resources");
    let settings = needs.resolve::<Profile>();
    let settings = settings.expect("listener settings");
    use dope_core::driver::life::creation::Attempt;
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, settings);
    assert!(executor.is_ok(), "listener executor");
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("create executor");
    let executor = executor.ok().expect("executor");
    let mut reports = Reports::<_>::new(settings, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

    executor.run(|executor| {
    let result = executor.enter_with(|mut session| {
        let config = config::Prepared {
            connections: Capacity::fixed::<2>(),
            per_ip: None,
            direct_flights: 1.into(),
            bind: ([127, 0, 0, 1], 0).into(),
            backlog: socket::Backlog::fixed::<2>(),
            stream: Default::default(),
            transport: Default::default(),
            egress: egress::Config::shared(egress::Credit::fixed::<0, 0>()),
        };
        let hash = session.hash_state(listener::Domain::DEFAULT);
        let result = listener::Install::<KeyTag<0>, Bundle<Tcp, Wire, Profile>, _>::new(
            config, domain::Local::new(()),
        ).with_driver(
            script::App {
                done: done.clone(),
                deferred: deferred.clone(),
                closed: closed.clone(),
                payload: Payload::Prefix,
                observe: direct,
            },
            hash, &mut session, session::Scope::driver,
            |server, _wait, session| {
                let addr = server.local_addr();
                let start = session.driver().driver_ref().clock().now().into();
                let result = session.with_at_for(&mut finish, Host::new(server), start, Duration::from_secs(2), |mut app| {
                    let first = peer::Peer::at(addr).try_connect().expect("first peer connect");
                    assert!(app.drive(start, Until { gate: &done, hits: 1 }).is_ok());
                    let second = peer::Peer::at(addr).try_connect().expect("second peer connect");
                    let wait = start.try_add(time::Duration::from_millis(50));
                    let wait = wait.expect("wait coordinate");
                    assert!(app.drive(wait, Until { gate: &deferred, hits: 1 }).is_ok());
                    assert_eq!(deferred.hits(), 1);
                    assert_eq!(done.hits(), 1);
                    let release = start.try_add(time::Duration::from_millis(101));
                    let release = release.expect("release coordinate");
                    assert!(app.drive(release, Until { gate: &done, hits: 2 }).is_ok());
                    assert_eq!(done.hits(), 2);
                    assert_eq!(closed.hits(), 1);
                    let finish = start.try_add(time::Duration::from_millis(251));
                    let finish = finish.expect("finish coordinate");
                    assert!(app.drive(finish, Until { gate: &closed, hits: 2 }).is_ok());
                    assert_eq!(closed.hits(), 2);
                    assert!(matches!(peer::Peer::try_read_all(&first), Ok(bytes) if bytes.is_empty()));
                    assert!(matches!(peer::Peer::try_read_all(&second), Ok(bytes) if bytes.is_empty()));
                });
                assert!(result.is_ok(), "manual listener teardown");
            });

        assert!(result.is_ok(), "manual listener installation");
    }, recovery::Policy::new(|event| reports.push(event), recover));
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert!(result.is_ok(), "manual listener runtime teardown");
    });
}
