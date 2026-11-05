use std::{
    cell::Cell,
    env::consts,
    net::{SocketAddr, TcpListener},
    os::fd::AsFd as _,
    pin::pin,
    task::Poll,
};

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{Epoch, Id, KeyTag, indices::SlotIndex, table::conn::Capacity},
        },
        io::socket::{self, Backlog, option::Switch},
    },
    manifold::{
        network::listener::{AcceptError, Domain},
        timing::Balanced,
    },
    net::{
        tcp::{StreamConfig, Tcp},
        wire::{Identity, domain},
    },
    runtime::executor::session::Completion,
};
use dope_core::{
    Backend, Errno,
    dope_sys::io::socket::{How, Socket as _},
    io::{
        Reclaimed,
        event::{batch::Flow, net::accept::Failure},
    },
};
use dope_fiber::{
    BlockOn as _,
    abi::Task,
    context::Context,
    net::server::{Config, Listener, Plan, accept::Error},
    task::storage::{scope::Scope, set::Set},
    wait,
};
use dope_test::{
    fibers,
    harness::{peer::Peer, scenario},
    reclaim::Reports,
};
use o3::depot::capacity::Count;

pub mod common;
use common::recover;

use common::{
    network::accept::{App, CancelFirst, Native},
    runtime::{self, Setup},
};

#[test]
fn accepts_after_exhaustion_and_cancellation() {
    for (no_delay, cancel_first) in [(None, false), (Some(Switch::On), false), (None, true)] {
        let held = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener address");
        let addr = held.local_addr();
        let capacity = if cancel_first {
            Capacity::fixed::<2>()
        } else {
            Capacity::fixed::<1>()
        };
        let plan = Plan::<Identity>::new(capacity, Identity);
        let setup = Setup::try_for_plan::<Balanced, _>(&plan).expect("driver configuration");
        let mut reports = Reports::new(setup.config(), [Id::fixed::<0>()]);
        setup.with_input(
            plan,
            |mut sess| {
                let seen = Cell::new(0);
                let failure = Cell::new(None);
                let accepted = Cell::new(false);
                let mut finish = Completion::new();
                let hash = sess.hash_state(Domain::DEFAULT);
                let plan = sess.storage();
                let storage = plan.try_build(sess.driver().storage).expect("port");
                let result = Listener::<KeyTag<0>, Tcp, Identity, _>::adopt(
                    &storage,
                    Config {
                        direct_flights: 0.into(),
                        bind: addr,
                        backlog: Backlog::fixed::<1>(),
                        stream: StreamConfig {
                            no_delay,
                            ..Default::default()
                        },
                        transport: Default::default(),
                        egress: Default::default(),
                        bound: held,
                        wire: domain::Local::new(Identity),
                    },
                    |cause| cause.eliminate(),
                    hash,
                    &mut sess,
                    |sess| sess.driver(),
                    |listener, sess| {
                        let handle = storage.handle();
                        sess.with_quiesce(
                            &mut finish,
                            App::new(listener, &seen, &failure, cancel_first.then_some(&accepted)),
                            scenario::TIMEOUT,
                            |mut app| {
                                let (mut io, peer) = if cancel_first {
                                    let mut tasks = pin!(
                                        Set::try_new(Count::new(2)).expect("accept task storage")
                                    );
                                    let (result, peer) = Scope::with(tasks.as_mut(), |tasks| {
                                        app.drive_for(
                                            scenario::TIMEOUT,
                                            CancelFirst {
                                                tasks,
                                                handle,
                                                accepted: &accepted,
                                                addr,
                                                peer: None,
                                                first: None,
                                            },
                                        )
                                    })
                                    .expect("surviving waiter must wake after cancellation");
                                    (
                                        result
                                            .ok()
                                            .expect("surviving waiter must accept the connection"),
                                        peer,
                                    )
                                } else {
                                    app.block_on(dope_fiber::fiber!('_ => async move {
                                        let peer = Peer::at(addr)
                                            .try_read(scenario::TIMEOUT)
                                            .expect("connect");
                                        let io = handle.accept().await.ok().expect("accept");
                                        (io, peer)
                                    }))?
                                };
                                app.block_on(dope_fiber::fiber!('_ => async move {
                                    assert!(io.write_all(b"x").await.is_ok(), "reply");
                                    let mut reply = [0];
                                    peer.read_exact(&mut reply).expect("accepted reply");
                                    assert_eq!(reply, *b"x");
                                }))
                            },
                        )
                        .expect("drive listener")
                        .expect("accept progresses");
                    },
                );
                assert!(result.is_ok(), "install listener");
                assert_eq!(failure.get(), None, "retain exhausted completion");
                assert_ne!(seen.get() & 1, 0, "accept credit exhausted before dispatch");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
        assert!(
            reports.events().expect("native reclamation").is_empty(),
            "unexpected reclamation: {reports:?}"
        );
    }
}

#[test]
fn wait_failure() {
    let plan = Plan::<Identity>::new(Capacity::fixed::<1>(), Identity);
    Setup::try_for_plan::<Balanced, _>(&plan)
        .expect("driver configuration")
        .with_turn(
            |turn, mut driver| {
                let storage = plan
                    .try_build(driver.driver().storage.reborrow())
                    .expect("listener port");
                let core = driver.driver_ref();
                let root = core
                    .ready()
                    .make_slot(
                        core.targets::<KeyTag<0>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch(),
                    )
                    .expect("ready slot");
                let handle = storage.handle();
                Context::with_target(root.target(), turn.turn().app(), driver, |mut context| {
                    {
                        let mut waiting = pin!(Task::new(handle.accept()));
                        assert!(context.as_mut().poll(waiting.as_mut()).is_pending());
                        let mut rejected = pin!(Task::new(handle.accept()));
                        assert!(matches!(
                            context.as_mut().poll(rejected.as_mut()),
                            Poll::Ready(Err(Error::Wait(wait::Error::QueueFull {
                                capacity,
                                wake,
                            }))) if capacity.get() == 1 && wake == root.target().into()
                        ));
                    }
                    let mut next = pin!(Task::new(handle.accept()));
                    assert!(context.poll(next.as_mut()).is_pending());
                });
            },
            recovery::Policy::new(|_| {}, recover),
            runtime::check_scope,
        );
}

#[test]
fn completion_failure() {
    assert_eq!(consts::OS, "linux", "listener shutdown requires Linux");
    let fault = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener address");
    let addr = fault.local_addr().expect("bound address");
    let bound = fault.try_clone().expect("listener fault descriptor");
    let plan = Plan::<Identity>::new(Capacity::fixed::<1>(), Identity);
    let cause = Cell::new(None);
    let reports = Cell::new(0);
    Setup::try_for_plan::<Balanced, _>(&plan)
        .expect("driver configuration")
        .with_input(
            plan,
            |mut sess| {
                let seen = Cell::new(0);
                let failure = Cell::new(None);
                let mut finish = Completion::new();
                let hash = sess.hash_state(Domain::DEFAULT);
                let storage = sess
                    .storage()
                    .try_build(sess.driver().storage)
                    .expect("listener port");
                let result = Listener::<KeyTag<0>, Native, Identity, _>::adopt(
                    &storage,
                    Config {
                        direct_flights: 0.into(),
                        bind: addr,
                        backlog: Backlog::fixed::<1>(),
                        stream: Default::default(),
                        transport: Default::default(),
                        egress: Default::default(),
                        bound,
                        wire: domain::Local::new(Identity),
                    },
                    |cause| cause.eliminate(),
                    hash,
                    &mut sess,
                    |sess| sess.driver(),
                    |listener, sess| {
                        Backend::shutdown::<Errno>(fault.as_fd(), How::READ)
                            .expect("stop native listener");
                        let handle = storage.handle();
                        sess.with_quiesce(
                            &mut finish,
                            App::new(listener, &seen, &failure, None),
                            scenario::TIMEOUT,
                            |mut app| {
                                let timeout = fibers::Harness::new(scenario::TIMEOUT);
                                for _ in 0..2 {
                                    let accepted = timeout
                                        .try_run(&mut app, handle.accept())
                                        .expect("terminal accept must wake");
                                    assert!(matches!(
                                        accepted,
                                        Err(Error::Listener(AcceptError::Completion(
                                            Failure::Syscall(error)
                                        ))) if Some(Failure::Syscall(error)) == cause.get()
                                    ));
                                }
                            },
                        )
                        .expect("listener retirement");
                        assert_eq!(failure.get(), None, "retain exhausted completion");
                        assert_ne!(seen.get() & 1, 0, "native accept completion");
                    },
                );
                assert!(result.is_ok(), "install listener");
            },
            recovery::Policy::new(
                |event| {
                    reports.set(reports.get() + 1);
                    if let Reclaimed::AcceptRoute {
                        cause: error,
                        flow: Flow::Last,
                        ..
                    } = event
                    {
                        cause.set(Some(error));
                    }
                },
                recover,
            ),
            runtime::check_scope,
        );
    assert_eq!(reports.get(), 1, "one exact native failure report");
    assert_eq!(cause.get(), Some(Failure::Syscall(Errno::INVALID_ARGUMENT)));
}
