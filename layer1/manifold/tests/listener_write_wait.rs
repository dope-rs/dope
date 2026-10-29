pub mod common;

use std::{convert::Infallible, fmt::Debug};

use dope_runtime::topology::error::Kind;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{
    io::Write as _,
    iter,
    time::{self, Duration},
};

use common::{
    manifold::listener::defer::{
        app,
        preamble::{self, FRAMES, HANDSHAKE, PREFIX},
        script::{self, BODY_LEN, Payload, Script, Until},
        wire::{Profile, Wire},
    },
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
    platform::wake,
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
    fibers::{Run, Wait},
    harness::{
        peer::{self, Peer},
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
                let request = b"open".to_vec();
                Peer::at(case.addr())
                    .with(
                        move |socket| peer::Half::try_run(socket, &request),
                        |peer| {
                            reached(case.try_wait(&ordered, 1), ordered.hits(), 1);
                            assert_eq!(done.hits(), 1);
                            assert!(matches!(case.try_wait(&done, 2), Ok(Wait::Reached)));
                            reached(case.try_wait(&closed, 1), closed.hits(), 1);
                            assert_eq!(
                                peer.finish()
                                    .expect("finish client peer")
                                    .expect("client peer failed"),
                                b"AbodyBbody"
                            );
                        },
                    )
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
    let [
        (wait_a, go_a),
        (wait_b, go_b),
        (wait_c, go_c),
        (wait_d, go_d),
    ] = [(); 4].map(|()| wake::Ends::blocking().expect("peer release pipe").split());
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
                Peer::at(case.addr())
                    .with(
                        move |socket| Script::new(wait_a).try_run(socket),
                        |first| {
                            Peer::at(case.addr())
                                .with(
                                    move |socket| Script::new(wait_b).try_run(socket),
                                    |second| {
                                        reached(case.try_wait(&done, 2), done.hits(), 2);
                                        Peer::at(case.addr())
                                            .with(
                                                move |socket| Script::new(wait_c).try_run(socket),
                                                |third| {
                                                    reached(
                                                        case.try_wait(&deferred, 1),
                                                        deferred.hits(),
                                                        1,
                                                    );
                                                    assert_eq!(done.hits(), 2);
                                                    go_a.notify()
                                                        .expect("first peer release failed");
                                                    go_b.notify()
                                                        .expect("second peer release failed");
                                                    reached(
                                                        case.try_wait(&done, 3),
                                                        done.hits(),
                                                        3,
                                                    );
                                                    reached(
                                                        case.try_wait(&closed, 2),
                                                        closed.hits(),
                                                        2,
                                                    );
                                                    assert_eq!(
                                                        first
                                                            .finish()
                                                            .expect("held peer finish")
                                                            .expect("held peer I/O")
                                                            .len(),
                                                        BODY_LEN + 1
                                                    );
                                                    assert_eq!(
                                                        second
                                                            .finish()
                                                            .expect("held peer finish")
                                                            .expect("held peer I/O")
                                                            .len(),
                                                        BODY_LEN + 1
                                                    );
                                                    Peer::at(case.addr())
                                                        .with(
                                                            move |socket| {
                                                                Script::new(wait_d).try_run(socket)
                                                            },
                                                            |fourth| {
                                                                reached(
                                                                    case.try_wait(&done, 4),
                                                                    done.hits(),
                                                                    4,
                                                                );
                                                                assert_eq!(deferred.hits(), 1);
                                                                go_c.notify().expect(
                                                                    "third peer release failed",
                                                                );
                                                                go_d.notify().expect(
                                                                    "fourth peer release failed",
                                                                );
                                                                reached(
                                                                    case.try_wait(&closed, 4),
                                                                    closed.hits(),
                                                                    4,
                                                                );
                                                                assert_eq!(
                                                                    third
                                                                        .finish()
                                                                        .expect("held peer finish")
                                                                        .expect("held peer I/O")
                                                                        .len(),
                                                                    BODY_LEN + 1
                                                                );
                                                                assert_eq!(
                                                                    fourth
                                                                        .finish()
                                                                        .expect("held peer finish")
                                                                        .expect("held peer I/O")
                                                                        .len(),
                                                                    BODY_LEN + 1
                                                                );
                                                            },
                                                        )
                                                        .expect("fourth peer spawn failed");
                                                },
                                            )
                                            .expect("third peer spawn failed");
                                    },
                                )
                                .expect("second peer spawn failed");
                        },
                    )
                    .expect("first peer spawn failed");
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
            |server, session| {
                let addr = server.local_addr();
                let start = session.driver().driver_ref().clock().now().into();
                let result = session.with_at_for(&mut finish, Host::new(server), start, Duration::from_secs(2), |mut app| {
                    let mut first = peer::Peer::at(addr).try_connect().expect("first peer connect");
                    assert!(app.drive(start, Until { gate: &done, hits: 1 }).is_ok());
                    let mut second = peer::Peer::at(addr).try_connect().expect("second peer connect");
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
                    assert!(matches!(peer::Peer::try_read_all(&mut first), Ok(bytes) if bytes.is_empty()));
                    assert!(matches!(peer::Peer::try_read_all(&mut second), Ok(bytes) if bytes.is_empty()));
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
