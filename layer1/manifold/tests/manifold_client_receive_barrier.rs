pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read as _, Write as _},
    num::NonZero,
    ptr,
    rc::Rc,
    time::Duration,
};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
    platform::wake,
};
use dope_manifold::{
    app::{Host, env::Bundle},
    client::session::{self, ReceiveMode, ingress},
    network::client::topology::plan,
    service::{Endpoint, Revision, Snapshot, health::Domain, sync},
    timing::Balanced,
};
use dope_net::{
    link::pool::recv::{input, reserve},
    tcp::Tcp,
    wire::{
        self,
        domain::{self, Local},
        failure,
        receive::{
            self,
            mode::{Disabled, access},
        },
    },
};
use dope_runtime::executor::{self, Executor, session::Completion};
use dope_test::{
    fibers,
    harness::{
        peer,
        signals::{self, Progress as _},
    },
    reclaim::Reports,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        Configure as _, error,
        switch::{
            barrier::{Barrier, Bytes, Gates, Mode},
            decode::Decode,
            retry::{Retry, Stages},
            terminal::{clean::Clean, queued},
        },
    },
};

#[test]
fn owner_survives_barrier() {
    for behavior in [Mode::Duplex, Mode::Yield, Mode::Blocked] {
        let mut finish = Completion::new();

        peer::TcpScript::with(|stream| {
                assert!(
                    stream.write_all(b"HRAW").is_ok(),
                    "write head and raw suffix"
                );
                let mut closed = Vec::new();
                assert!(
                    stream.read_to_end(&mut closed).is_ok(),
                    "wait for transition connection close"
                );
            }, |peer| {
            let address = peer.addr();
            let plan = (plan::Plan::<KeyTag<0>, 1>::new(
                1,
                plan::Spec::new(Duration::from_millis(10), Duration::from_millis(100))
                    .expect("valid retry bounds"),
            ))
            .expect("connector capacity");
            let config = (plan.configured()).expect("configure executor");
            use dope_core::driver::life::creation::Attempt;
            let mut create = Attempt::new();
            let result = Executor::new(&mut create, config);
            result
                .as_ref()
                .map_err(|error| error.kind())
                .expect("create executor");
            let Ok(executor) = result else {
                return;
            };
            let mut reclaim1 = Reports::<_>::new(config, [Id::fixed::<0>()]);

            executor.with_storage(()).run(|executor| {
    let result = executor.enter_with(|mut runtime| {
                use executor::session::Scope;
                let mode = Rc::new(Cell::new(ReceiveMode::Framed));
                let target = Rc::new(Cell::new(None));
                let bytes = Bytes {
                    suffix: Rc::new(Cell::new(ptr::null())),
                    raw: Rc::new(Cell::new(Vec::new())),
                };
                let head = signals::Gate::new();
                let duplex = signals::Gate::new();
                let held = signals::Gate::new();
                let retired = signals::Gate::new();
                let prefix = signals::Gate::new();
                let growth = signals::Gate::new();
                let settled = Rc::new(Cell::new(0));
                let audit = Rc::new(Report::default());
                let app = Barrier {
                    audit: Rc::clone(&audit),

                    decode: Decode {
                        prefix: prefix.clone(),
                        growth: growth.clone(),
                        attempts: NonZero::<usize>::MIN.saturating_add(1),
                    },
                    attempt: Cell::new(None),
                    mode: mode.clone(),
                    bytes: bytes.clone(),
                    gates: Gates {
                        target: target.clone(),
                        settlements: settled.clone(),
                        head: head.clone(),
                        duplex: duplex.clone(),
                        quarantine: held.clone(),
                        retired: retired.clone(),
                    },
                    behavior,
                    pending: false,
                };
                let source = Snapshot::from_one(Revision::new(1), Endpoint::new(address, address));
                let backoff = runtime.hash_state(Domain::DEFAULT);
                let setup = session::Setup::new(app, ingress::Plan::none());
                let result = plan.bind(Local::new(wire::Identity), backoff).install::<
                        _, _, sync::Preserve, _,
                        Bundle<Tcp, wire::Identity, Balanced>,
                        1,
                        _,
                        _,
                    >(
                    setup,
                    source,
                    &mut runtime,
                    Scope::driver,
                    |host, runtime| {
            let result = runtime.with_quiesce(&mut finish, Host::new(host), Duration::from_secs(2), |mut running| {
                if matches!(behavior, Mode::Blocked) {
                    reached(fibers::TEST.try_wait(&mut running, &held, 1), &audit);
                    assert_eq!(head.hits(), 0);
                    assert_eq!(settled.get(), 0);
                    target.get().expect("generation readiness target").wake();
                }
                reached(fibers::TEST.try_wait(&mut running, &head, 1), &audit);
                assert!(bytes.snapshot().is_empty(), "barrier must retain the suffix");
                match behavior {
                    Mode::Duplex => {
                        mode.set(ReceiveMode::Duplex);
                        let target = (target.get()).expect("generation readiness target");
                        target.wake();
                        reached(fibers::TEST.try_wait(&mut running, &duplex, 1), &audit);
                        assert_eq!(bytes.snapshot().as_slice(), b"RAW");
                    }
                    Mode::Yield | Mode::Blocked => {
                        reached(fibers::TEST.try_wait(&mut running, &retired, 1), &audit);
                        assert_eq!(duplex.hits(), 0);
                    }
                }
            });
            assert!(result.is_ok(), "application teardown");
            audit.checked((0, 0));
            match behavior {
                Mode::Duplex => assert_eq!(bytes.snapshot().as_slice(), b"RAW"),
                Mode::Yield | Mode::Blocked => assert_eq!(retired.hits(), 1),
            }
            assert_eq!(prefix.hits(), 0);
            assert_eq!(growth.hits(), 0);
                    },
                );
                assert!(result.is_ok(), "create transition connector");
            }, recovery::Policy::new(|event| reclaim1.push(event), recover));
        assert!(
            matches!(reclaim1.events(), Ok(events) if events.is_empty()),
            "unexpected reclaimed operations: {reclaim1:?}"
        );

        assert!(result.is_ok(), "executor teardown");

        assert!(peer.finish().is_ok(), "finish transition peer");
        });
        }).expect("spawn transition peer");
    }
}

#[test]
fn same_owner_retries() {
    run::<wire::Identity>(NonZero::<usize>::MIN, false, None, None, false);
}

#[test]
fn close_discards_retry() {
    run::<wire::Identity>(
        NonZero::<usize>::MIN,
        false,
        Some(signals::Gate::new()),
        None,
        true,
    );
}

#[test]
fn yield_precedes_receive() {
    run::<wire::Identity>(
        NonZero::<usize>::MIN.saturating_add(99_999),
        false,
        None,
        None,
        false,
    );
}

#[test]
fn empty_rebind_progress() {
    run::<wire::Identity>(NonZero::<usize>::MIN, true, None, None, false);
}

#[test]
fn eof_waits_for_input() {
    run::<Clean<true>>(
        NonZero::<usize>::MIN,
        false,
        Some(signals::Gate::new()),
        None,
        false,
    );
}

#[test]
fn queued_error_follows_eof() {
    use dope_manifold::client::close;
    use dope_test::harness::observe::Trace;

    let terminal = Rc::new(queued::State {
        error: dope_core::Errno::try_from(libc::ECONNRESET).expect("receive reset errno"),
        queued: signals::Gate::new(),
        cancelled_flow: Cell::new(None),
        events: Trace::new(),
    });
    run::<Clean<true>>(
        NonZero::<usize>::MIN,
        false,
        Some(signals::Gate::new()),
        Some(Rc::clone(&terminal)),
        false,
    );
    assert_eq!(terminal.queued.hits(), 1);
    assert_eq!(
        terminal.cancelled_flow.get(),
        Some(dope_core::io::event::batch::Flow::Last)
    );
    assert_eq!(
        &*terminal.events.values().expect("terminal delivery order"),
        &[
            queued::Event::Input,
            queued::Event::Eof,
            queued::Event::Failed(terminal.error),
            queued::Event::Closed(close::Reason::Transport),
        ],
    );
}

fn run<W>(
    count: NonZero<usize>,
    prefix: bool,
    retired: Option<signals::Gate>,
    terminal: Option<Rc<queued::State>>,
    closing: bool,
) where
    W: input::Execution
        + wire::Spec<OpenFailure = failure::Never, TerminalFailure = failure::Never>
        + 'static,
    W::Bootstrap: domain::Init<W>,
    W::Receive: receive::Policy<W, Access = access::Direct, Credit = Disabled>,
    <W::Receive as receive::Policy<W>>::Reserve: reserve::Choice<W, W::Retained>,
    for<'d> W::Binding<'d, KeyTag<0>>: Default,
{
    let mut finish = Completion::new();
    let want = (NonZero::<u32>::try_from(count)).expect("input count must fit the completion gate");
    let attempts = (count.checked_add(1)).expect("input count must leave room for its first retry");

    let (next, resume) = wake::Ends::blocking().expect("peer resume channel").split();
    peer::TcpScript::with(
        move |stream| {
            if prefix {
                stream.write_all(b"P").expect("write partial prefix");
                next.wait_timeout(Duration::from_secs(2))
                    .expect("prefix consumed");
            }
            let frames = vec![b'H'; count.get()];
            stream.write_all(&frames).expect("write causal frames");
            let mut closed = Vec::new();
            stream
                .read_to_end(&mut closed)
                .expect("wait for causal connection close");
        },
        |peer| {
            let address = peer.addr();
            let plan = (plan::Plan::<KeyTag<0>, 1>::new(
                1,
                plan::Spec::new(Duration::from_millis(10), Duration::from_millis(100))
                    .expect("valid retry bounds"),
            ))
            .expect("connector capacity");
            let config = (plan.configured()).expect("configure executor");
            use dope_core::driver::life::creation::Attempt;
            let mut create = Attempt::new();
            let result = Executor::new(&mut create, config);
            result
                .as_ref()
                .map_err(|error| error.kind())
                .expect("create executor");
            let Ok(executor) = result else {
                return;
            };
            let mut reclaim1 = Reports::<_>::new(config, [Id::fixed::<0>()]);

            executor.with_storage(()).run(|executor| {
                let result = executor.enter_with(
                    |mut runtime| {
                        use executor::session::Scope;
                        let target = Rc::new(Cell::new(None));
                        let suffix = Rc::new(Cell::new(ptr::null()));
                        let tries = Rc::new(Cell::new(0));
                        let released = Rc::new(Cell::new(false));
                        let blocked = signals::Gate::new();
                        let commit = signals::Gate::new();
                        let cleared = signals::Gate::new();
                        let partial = signals::Gate::new();
                        let growth = signals::Gate::new();
                        let audit = Rc::new(Report::default());
                        let app = Retry {
                            audit: Rc::clone(&audit),

                            decode: Decode {
                                attempts,
                                prefix: partial.clone(),
                                growth: growth.clone(),
                            },
                            target: target.clone(),
                            suffix,
                            tries: tries.clone(),
                            released: released.clone(),
                            closing,
                            stages: Stages {
                                blocked: blocked.clone(),
                                commit: commit.clone(),
                                cleared: cleared.clone(),
                                retired: retired.clone(),
                                terminal: terminal.clone(),
                            },
                        };
                        let source =
                            Snapshot::from_one(Revision::new(1), Endpoint::new(address, address));
                        let backoff = runtime.hash_state(Domain::DEFAULT);
                        let setup = session::Setup::new(app, ingress::Plan::none());
                        let result = plan
                            .bind(Local::<W, KeyTag<0>>::new(Default::default()), backoff)
                            .install::<_, _, sync::Preserve, _, Bundle<Tcp, W, Balanced>, 1, _, _>(
                                setup,
                                source,
                                &mut runtime,
                                Scope::driver,
                                |host, runtime| {
                                    let host = Host::new(queued::Host {
                                        inner: host,
                                        state: terminal.clone(),
                                        pending: None,
                                    });
                                    let result = runtime.with_quiesce(
                                        &mut finish,
                                        host,
                                        Duration::from_secs(2),
                                        |mut running| {
                                            if prefix {
                                                reached(
                                                    fibers::TEST.try_wait(
                                                        &mut running,
                                                        &partial,
                                                        1,
                                                    ),
                                                    &audit,
                                                );
                                                assert_eq!(growth.hits(), 1);
                                                resume.notify().expect("release next chunk");
                                            }
                                            reached(
                                                fibers::TEST.try_wait(&mut running, &blocked, 1),
                                                &audit,
                                            );
                                            if closing {
                                                let retired =
                                                    retired.as_ref().expect("retirement gate");
                                                reached(
                                                    fibers::TEST.try_wait(&mut running, retired, 1),
                                                    &audit,
                                                );
                                                assert_eq!(retired.hits(), 1);
                                                assert_eq!(cleared.hits(), 1);
                                                assert_eq!(commit.hits(), 0);
                                                return;
                                            }
                                            if let Some(terminal) = &terminal {
                                                reached(
                                                    fibers::TEST.try_wait(
                                                        &mut running,
                                                        &terminal.queued,
                                                        1,
                                                    ),
                                                    &audit,
                                                );
                                                assert!(
                                                    terminal
                                                        .events
                                                        .values()
                                                        .expect("held EOF observations")
                                                        .is_empty()
                                                );
                                            }
                                            assert_eq!(tries.get(), 1);
                                            assert_eq!(commit.hits(), 0);
                                            if let Some(retired) = &retired {
                                                assert_eq!(retired.hits(), 0);
                                            }
                                            let target = (target.get())
                                                .expect("generation readiness target");
                                            released.set(true);
                                            target.wake();
                                            reached(
                                                fibers::TEST.try_wait(
                                                    &mut running,
                                                    &commit,
                                                    want.get(),
                                                ),
                                                &audit,
                                            );
                                            assert_eq!(tries.get(), attempts.get());
                                            if let Some(retired) = &retired {
                                                reached(
                                                    fibers::TEST.try_wait(&mut running, retired, 1),
                                                    &audit,
                                                );
                                            }
                                        },
                                    );
                                    assert!(result.is_ok(), "application teardown");
                                    audit.checked((0, 0));
                                    assert_eq!(
                                        tries.get(),
                                        if closing { 1 } else { attempts.get() }
                                    );
                                    assert_eq!(partial.hits(), u32::from(prefix));
                                    assert_eq!(growth.hits(), u32::from(prefix));
                                },
                            );
                        assert!(result.is_ok(), "create causal connector");
                    },
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                );
                assert!(
                    matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                    "unexpected reclaimed operations: {reclaim1:?}"
                );

                assert!(result.is_ok(), "executor teardown");

                peer.finish().expect("finish causal peer");
            });
        },
    )
    .expect("spawn causal peer");
}

fn reached(wait: Result<fibers::Wait, fibers::Run>, audit: &Report<error::Error, 16>) {
    audit.checked((0, 0));
    assert!(
        matches!(wait, Ok(fibers::Wait::Reached)),
        "progress: {wait:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
