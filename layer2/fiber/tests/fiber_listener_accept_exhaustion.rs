use std::{
    cell::Cell,
    convert::Infallible,
    env::consts,
    io::Read as _,
    net::{SocketAddr, TcpListener},
    os::fd::AsFd as _,
    pin::pin,
    task::Poll,
};

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{Epoch, KeyTag, indices::SlotIndex, table::conn::Capacity},
        },
        io::socket::{Backlog, option::Switch},
        platform::wake,
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
    wait,
};
use dope_test::{
    fibers,
    harness::{
        peer::Peer,
        scenario::{
            self,
            run::{Entry, Task as _},
        },
    },
};

pub mod common;

use common::{
    network::{
        accept::{App, Native},
        reserve::Reservation,
    },
    runtime::{self, Setup},
};

#[test]
fn accepts_after_exhaustion() {
    for no_delay in [None, Some(Switch::On)] {
        let held = Reservation::bind().expect("listener address");
        let addr = held.addr();
        let (wait, start) = wake::Ends::blocking().expect("accept release").split();
        let peer = Entry(move || {
            wait.wait_timeout(scenario::TIMEOUT).expect("accept waiter");
            let mut stream = Peer::at(addr).try_read(scenario::TIMEOUT).expect("connect");
            let mut byte = [0];
            stream.read_exact(&mut byte).expect("accepted reply");
            byte
        });
        let ((), reply) = peer
            .with(|| {
                let mut report = None;
                let plan = Plan::<Identity>::new(Capacity::fixed::<1>(), Identity);
                Setup::try_for_plan::<Balanced, _>(&plan)
                    .expect("driver configuration")
                    .with_input(
                        plan,
                        |mut sess| {
                            let seen = Cell::new(0);
                            let failure = Cell::new(None);
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
                                    bound: held.into_tcp(),
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
                                        App::new(listener, &seen, &failure),
                                        scenario::TIMEOUT,
                                        |mut app| {
                                            app.block_on(dope_fiber::fiber!('_ => async move {
                                    start.notify().expect("release peer");
                                    let mut io = handle.accept().await.ok().expect("accept");
                                    assert!(io.write_all(b"x").await.is_ok(), "reply");
                                }))
                                        },
                                    )
                                    .expect("drive listener")
                                    .expect("accept progresses");
                                },
                            );
                            assert!(result.is_ok(), "install listener");
                            assert_eq!(failure.get(), None, "retain exhausted completion");
                            assert_ne!(
                                seen.get() & 1,
                                0,
                                "accept credit exhausted before dispatch"
                            );
                        },
                        recovery::Policy::new(
                            |event| {
                                report.get_or_insert(event);
                            },
                            recover,
                        ),
                        self::finish,
                    );
                assert!(report.is_none(), "unexpected reclamation: {report:?}");
            })
            .expect("peer worker");
        assert_eq!(reply.expect("peer join"), *b"x");
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
            self::finish,
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
                            App::new(listener, &seen, &failure),
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
            self::finish,
        );
    assert_eq!(reports.get(), 1, "one exact native failure report");
    assert_eq!(cause.get(), Some(Failure::Syscall(Errno::INVALID_ARGUMENT)));
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
