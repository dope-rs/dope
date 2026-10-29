pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{self, Read as _, Write as _},
    net::Shutdown,
    rc::Rc,
    time::Duration,
};

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::{Id, KeyTag},
};
use dope_manifold::{
    app::{Host, env::Bundle, receive},
    client::{
        close::Reason,
        session::{
            self,
            codec::{Stateless, eof},
            ingress,
        },
    },
    network::client::topology::plan,
    service::{Endpoint, Revision, Snapshot, health, sync},
    timing::{Balanced, Throughput},
};
use dope_net::{
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::executor::{
    self, Executor,
    session::{Completion, Scope},
};
use dope_test::{
    fibers,
    harness::{
        observe::Trace,
        peer::{self, Peer},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::{base::time::Positive, buffer::storage::Shared};

use crate::common::{
    contract::{
        Report,
        codec::{self, errors},
    },
    manifold::client::{
        Configure as _,
        finish::{
            close::{
                members::Until,
                observe::{Events, Session},
                run::{Exhausted, Run, serve},
            },
            half::{AFTER_EOF, Action, Half, Progress, RESPONSE},
        },
        switch::terminal::clean::Clean,
    },
    scenario::case::{Connector, Value},
};

#[test]
fn parser_error_redials() {
    run(Run {
        codec: codec::Outbound,
        error: Some(errors::Inbound),
        reason: Reason::Protocol,
    });
}

#[test]
fn capacity_is_typed() {
    run(Run {
        codec: Exhausted,
        error: None,
        reason: Reason::Ingress(receive::Failure::ParseCapacity),
    });
}

fn run<C: Stateless + eof::Finish>(case: Run<C>)
where
    C::Error: PartialEq,
{
    let mut finish = Completion::new();
    peer::AcceptScript::with(serve, |peer| {
        let addr = peer.addr();
        let gate = Gate::new();
        let plan = (plan::Plan::<KeyTag<0>, 1, plan::Standard, plan::Local<C::Staging>>::new(
            1,
            plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
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

                    let events = Rc::new(Cell::new(Events::default()));
                    let errors = Rc::new(Trace::new());
                    let backoff = runtime.hash_state(health::Domain::DEFAULT);
                    let audit = Rc::new(Report::default());
                    let target = Session {
                        audit: audit.clone(),

                        codec: case.codec,
                        errors: errors.clone(),
                        events: events.clone(),
                        gate: gate.clone(),
                    };
                    let setup = session::Setup::new(target, ingress::Plan::none());
                    let set = Snapshot::from_one(Revision::new(1), Endpoint::new(addr, addr));
                    let result =
                        plan.bind(domain::Local::new(Identity), backoff)
                            .install::<_, _, sync::Preserve, _, Bundle<Tcp, Identity, Balanced>, 16, _, _>(
                        setup,
                        set,
                        &mut runtime,
                        Scope::driver,
                        |host, runtime| {
                            let result = runtime.with_quiesce(&mut finish,
                                Host::new(host),
                                Duration::from_secs(2),
                                |mut app| {
                                    assert_eq!(fibers::TEST.try_wait(&mut app, &gate, 2).expect("driver wait"), fibers::Wait::Reached);
                                    let events = Events::snapshot(&events);
                                    assert_eq!(events.closed.len(), 1);
                                    assert!((events.closed[0].1) == (case.reason));
                                    let old = events.conn[0];
                                    let new = events.conn[1];
                                    assert!((old.index()) == (new.index()));
                                    assert!(old != new);
                                    assert_eq!(events.replies, Some(0));
                                },
                            );
                            assert!(result.is_ok(), "application teardown");
                        },
                    );
                    assert!(result.is_ok(), "create connector");
                    audit.checked((0, 0));
                    assert!(&*errors.values().expect("codec observations") == case.error.as_slice());
                    let observed = Events::snapshot(&events);
                    assert_eq!(gate.hits() as usize, observed.conn.len());
                    assert!(observed.replies.is_some(), "reply count overflowed");
                    assert!(observed.is_ordered(), "{:?}", (&observed.conn, &observed.closed, &observed.quarantined));
                },
                recovery::Policy::new(|event| reclaim1.push(event), recover),
            );
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            assert!(result.is_ok(), "executor teardown");

            Peer::at(addr)
                .try_connect()
                .expect("release protocol-error peer");
            let finished = (peer.finish()).expect("finish protocol-error peer");
            finished.expect("protocol-error peer I/O");
        });
    }).expect("spawn protocol-error peer");
}

#[test]
fn retained_eof_send_close() {
    for action in [Action::Send, Action::Close] {
        let mut finish = Completion::new();

        peer::TcpScript::with(
            |stream| {
                stream.write_all(RESPONSE)?;
                stream.shutdown(Shutdown::Write)?;
                let mut got = Vec::new();
                stream.read_to_end(&mut got)?;
                Ok::<_, io::Error>(got)
            },
            |peer| {
                let address = peer.addr();

                let conn = Gate::new();
                let closed = Gate::new();
                let received = Rc::new(Cell::new(Vec::new()));
                let parked = Gate::new();
                let release = Gate::new();
                let audit = Rc::new(Report::default());

                let planned = Connector::<1>::new(
                    address,
                    Duration::from_millis(10),
                    Duration::from_millis(50),
                )
                .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Value::new(Half {
                    action,
                    progress: Progress {
                        eof: false,
                        issued: false,
                        sent: false,
                        parked: parked.clone(),
                        release: release.clone(),
                    },
                    response: match action {
                        Action::Send => Some(Shared::try_copy(AFTER_EOF).expect("EOF response")),
                        Action::Close | Action::Timeout => None,
                    },
                    audit: audit.clone(),

                    conn: conn.clone(),
                    closed: closed.clone(),
                    input: received.clone(),
                }))
                .expect("prepare actual client layout");
                let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

                planned
                    .with_run(
                        &mut finish,
                        |case| {
                            assert_eq!(
                                case.try_wait(&conn, 1).expect("progress wait"),
                                fibers::Wait::Reached
                            );
                            assert_eq!(
                                case.try_wait(&parked, 2).expect("progress wait"),
                                fibers::Wait::Reached
                            );
                            assert_eq!(closed.hits(), 0);
                            release.hit();
                            assert_eq!(
                                case.try_wait(&closed, 1).expect("progress wait"),
                                fibers::Wait::Reached
                            );
                        },
                        recovery::Policy::new(|event| reclaim1.push(event), recover),
                        |result| result.expect("native lifecycle"),
                    )
                    .expect("application scenario");
                assert!(
                    matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                    "unexpected reclaimed operations: {reclaim1:?}"
                );

                assert_eq!(received.take().as_slice(), RESPONSE);
                audit.checked((usize::from(matches!(action, Action::Send)), 0));
                assert_eq!(conn.hits(), 1);
                assert_eq!(closed.hits(), 1);
                let got = peer.finish().expect("peer finish").expect("peer I/O");
                assert_eq!(
                    got,
                    match action {
                        Action::Send => AFTER_EOF,
                        Action::Close | Action::Timeout => b"",
                    }
                );
            },
        )
        .expect("spawn peer");
    }
}

#[test]
fn held_eof_timeout_closes() {
    let mut finish = Completion::new();
    peer::TcpScript::with(
        |stream| {
            stream.write_all(b"x")?;
            let mut got = Vec::new();
            stream.read_to_end(&mut got)?;
            Ok::<_, io::Error>(got)
        },
        |peer| {
            let address = peer.addr();
            let conn = Gate::new();
            let held = Gate::new();
            let closed = Gate::new();
            let input = Rc::new(Cell::new(Vec::new()));
            let audit = Rc::new(Report::default());
            let lifetime = Positive::from_millis::<100>();
            let plan = plan::Plan::<KeyTag<0>, 1>::new(
                1,
                plan::Spec::new(Duration::from_secs(1), Duration::from_secs(1))
                    .expect("valid retry bounds"),
            )
            .expect("connector capacity")
            .with_ttl(lifetime);
            let config = plan.configured().expect("configure executor");
            let mut create = Attempt::new();
            let result = Executor::new(&mut create, config);
            result
                .as_ref()
                .map_err(|error| error.kind())
                .expect("create executor");
            let executor = result.ok().expect("create executor");
            let mut reclaim = Reports::<_>::new(config, [Id::fixed::<0>()]);

            executor.with_storage(()).run(|executor| {
                let result = executor.enter_with(
                    |mut runtime| {
                        let setup = session::Setup::new(
                            Half {
                                action: Action::Timeout,
                                progress: Progress {
                                    eof: false,
                                    issued: false,
                                    sent: false,
                                    parked: held.clone(),
                                    release: Gate::new(),
                                },
                                response: None,
                                audit: audit.clone(),
                                conn: conn.clone(),
                                closed: closed.clone(),
                                input: input.clone(),
                            },
                            ingress::Plan::none(),
                        );
                        let backoff = runtime.hash_state(health::Domain::DEFAULT);
                        let set =
                            Snapshot::from_one(Revision::new(1), Endpoint::new(address, address));
                        let result = plan.bind(domain::Local::new(()), backoff).install::<
                            _,
                            _,
                            sync::Preserve,
                            _,
                            Bundle<Tcp, Clean<true>, Throughput>,
                            1,
                            _,
                            _,
                        >(
                            setup,
                            set,
                            &mut runtime,
                            Scope::driver,
                            |host, runtime| {
                                let now = runtime.driver().driver_ref().clock().now();
                                let result = runtime.with_at_for(
                                    &mut finish,
                                    Host::new(host),
                                    now.into(),
                                    Duration::from_secs(2),
                                    |mut app| {
                                        let progress = app.drive_for(
                                            now.into(),
                                            Duration::from_secs(2),
                                            Until(|| held.hits() == 1),
                                        )
                                        .map(|fixed| fixed.into_output());
                                        assert!(
                                            progress.is_ok(),
                                            "hold plaintext before EOF: {progress:?}, counts {:?}",
                                            [conn.hits(), held.hits(), closed.hits()],
                                        );
                                        assert_eq!(closed.hits(), 0);
                                        app.drive_for(
                                            now.clamp_add(lifetime.get()),
                                            Duration::from_secs(2),
                                            Until(|| closed.hits() == 1),
                                        )
                                        .expect("timeout retires held EOF")
                                        .into_output();
                                    },
                                );
                                assert!(result.is_ok(), "application teardown");
                            },
                        );
                        assert!(result.is_ok(), "install EOF connector");
                    },
                    recovery::Policy::new(|event| reclaim.push(event), recover),
                );
                assert!(result.is_ok(), "executor teardown");
            });

            assert!(
                matches!(reclaim.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim:?}"
            );
            assert_eq!([conn.hits(), held.hits(), closed.hits()], [1; 3]);
            assert_eq!(input.take(), b"x");
            audit.checked((0, 0));
            assert!(
                peer.finish()
                    .expect("peer finish")
                    .expect("peer I/O")
                    .is_empty()
            );
        },
    )
    .expect("spawn EOF peer");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
