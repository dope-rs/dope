use crate::common::manifold::client::error;
pub mod common;

use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    fmt::Debug,
    mem,
    rc::Rc,
    time::Duration,
};

use dope_core::{
    Errno,
    driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
};
use dope_manifold::{
    app::env::Bundle,
    client::{close::Reason, deadline, session::policy::output::Cancel},
};
use dope_net::{link::egress, tcp::Tcp, wire::Identity};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers,
    harness::{
        observe::Trace,
        peer,
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use o3::{
    base::Call as _,
    buffer::storage::{self, Shared},
};

use crate::common::{
    contract::Report,
    manifold::client::send::{
        Flight, Payload, profile,
        reject::{self, Reject},
        wait::{self, PAYLOAD_BYTES, Wait},
        wire::Wire,
    },
    scenario::case::{Connector, Value},
};

#[test]
fn release_before_wake() {
    let phase = Cell::new(0);
    let cancel = Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
        drop::<Payload<'_>>,
        |phase: &Cell<u8>| {
            assert_eq!(phase.replace(2), 1, "notification follows payload release");
        },
    );
    assert_eq!(mem::size_of_val(&cancel), 0);
    cancel.call((&phase, Payload(&phase), ()));
    assert_eq!(phase.get(), 2);
}

#[test]
fn impossible_send() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<1>(
        |_| {},
        |address, release| {
            let state = LocalGate::new();
            let closed = state.handle();
            let reason = Cell::new(None);

            let audit = Rc::new(Report::default());

            let planned = Connector::<{ reject::MAX }>::new(
                address,
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, profile::Slow>>(Value::new(Reject {
                create: || {
                    let value = Shared::try_copy(b"xx").expect("shared request payload");
                    egress::Load::new(value)
                        .try_nonempty()
                        .expect("nonempty request payload")
                },
                audit: audit.clone(),

                pending: Cell::new(false),
                closed,
                reason: &reason,
            }))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        )
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
            assert_eq!(closed.hits(), 1);
            assert_eq!(
                (audit.sent.get(), audit.cancelled.get()),
                (Some(0), Some(0))
            );
            assert!(matches!(
                reason.get(),
                Some(Reason::Egress(egress::reserve::exact::Failure::Lane { .. }))
            ));
            release.try_fire().expect("release scripted peers");
        },
    )
    .expect("gated peers");
}

#[test]
fn cancels_kernel_flight() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<1>(
        |_| {},
        |address, release| {
            let cancel = LocalGate::new();
            let canceled = cancel.handle();
            let state = LocalGate::new();
            let closed = state.handle();
            let reason = Cell::new(None);
            let payload = (storage::Shared::try_from(&mut vec![0; PAYLOAD_BYTES]))
                .expect("owned payload retains its allocation");

            let audit = Rc::new(Report::default());

            let planned = Connector::<{ wait::MAX }>::new(
                address,
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .egress(egress::Config::shared(egress::Credit::new(
                egress::Entries::new(4),
                egress::Resident::new(PAYLOAD_BYTES as u32),
            )))
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, profile::Fast>>(Value::new(
                Flight::new(payload, canceled, closed, &reason, audit.clone()),
            ))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        )
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
            assert_eq!(canceled.hits(), 1);
            assert_eq!(closed.hits(), 1);
            assert!((reason.get()) == (Some(Reason::Timeout(deadline::Kind::Send))));
            release.try_fire().expect("release scripted peers");
        },
    )
    .expect("gated peers");
}

#[test]
fn starts_before_flight() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<1>(
        |_| {},
        |address, release| {
            let cancel = LocalGate::new();
            let canceled = cancel.handle();
            let state = LocalGate::new();
            let closed = state.handle();
            let reason = Cell::new(None);
            let payload = (Shared::try_copy(b"x")).expect("allocate one-byte payload");

            let audit = Rc::new(Report::default());

            let planned =
                Connector::<{ wait::MAX }>::new(
                    address,
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                )
                .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
                .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Fast>>(Value::new(
                    Flight::new(payload, canceled, closed, &reason, audit.clone()),
                ))
                .expect("prepare actual client layout");
            let mut reclaim2 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        )
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
            assert_eq!(canceled.hits(), 1);
            assert_eq!(closed.hits(), 1);
            assert!((reason.get()) == (Some(Reason::Timeout(deadline::Kind::Send))));
            release.try_fire().expect("release scripted peers");
        },
    )
    .expect("gated peers");
}

#[test]
fn starts_on_capacity_wait() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<2>(
        |_| {},
        |address, release| {
            let index = Rc::new(Cell::new(None));
            let reasons = Rc::new(Trace::new());
            let seen = Gate::new();
            let closed = Gate::new();

            let audit = Rc::new(Report::default());

            let planned =
                Connector::<2>::new(address, Duration::from_secs(1), Duration::from_secs(1))
                    .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
                    .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Fast>>(Value::new(Wait {
                        create: || {
                            let value = Shared::try_copy(b"x").expect("shared request payload");
                            egress::Load::new(value)
                                .try_nonempty()
                                .expect("nonempty request payload")
                        },
                        audit: audit.clone(),

                        pending: RefCell::new([None, None]),
                        index: index.clone(),
                        reasons: reasons.clone(),
                        seen: seen.clone(),
                        closed: closed.clone(),
                    }))
                    .expect("prepare actual client layout");
            let mut reclaim3 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&seen, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        );
                    },
                    recovery::Policy::new(|event| reclaim3.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert!(
                matches!(reclaim3.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim3:?}"
            );

            checked(&audit);
            let reasons = reasons.values().expect("disconnect reasons");
            assert!((1..=2).contains(&reasons.len()));
            let waiting = (index.get()).expect("one waiting source");
            assert!(reasons.contains(&(waiting, Reason::Timeout(deadline::Kind::Send))));
            release.try_fire().expect("release scripted peers");
        },
    )
    .expect("gated peers");
}

#[test]
fn retire_keeps_deadline() {
    check_retirement(false, None);
}

#[test]
fn shutdown_keeps_deadline() {
    check_retirement(true, None);
}

#[test]
fn recv_error_after_close() {
    check_retirement(
        false,
        Some(Errno::try_from(libc::ECONNRESET).expect("receive reset errno")),
    );
}

fn check_retirement(shutdown: bool, fault: Option<Errno>) {
    use dope_core::{
        driver::{
            life::creation::Attempt,
            settings::{self, claim::Needs},
        },
        io::event::batch,
    };
    use dope_manifold::{
        app::Host,
        client::{
            close::LocalOrigin,
            session::{self, ingress},
        },
        network::client::topology::plan,
        service::{Endpoint, Revision, Snapshot, health, sync},
    };
    use dope_net::wire::domain;
    use dope_runtime::executor::{Executor, session::Scope};

    use crate::common::manifold::{
        client::switch::terminal::queued, listener::finish::close::wire,
    };

    peer::TcpScript::try_gated::<1>(
        |_| {},
        |address, peer| {
            let mut finish = Completion::new();
            let ready = LocalGate::new();
            let ready = ready.handle();
            let canceled = LocalGate::new();
            let canceled = canceled.handle();
            let closed = LocalGate::new();
            let closed = closed.handle();
            let entered = Gate::new();
            let release = Gate::new();
            let writes = Rc::new(Trace::new());
            let reason = Cell::new(None);
            let audit = Rc::new(Report::default());
            let fault = fault.map(|error| {
                Rc::new(queued::State {
                    error,
                    queued: Gate::new(),
                    cancelled_flow: Cell::new(None),
                    events: Trace::new(),
                })
            });
            let plan = plan::Plan::<KeyTag<0>, 1>::new(
                1,
                plan::Spec::new(Duration::from_secs(1), Duration::from_secs(1))
                    .expect("retry bounds"),
            )
            .expect("client layout");
            let settings = Needs::from_tree(&plan)
                .and_then(|needs| {
                    needs.apply_to(settings::Config::for_profile::<profile::Fast>())
                })
                .expect("client settings");
            let mut create = Attempt::new();
            let executor = Executor::new(&mut create, settings);
            executor.as_ref().map_err(|error| error.kind()).expect("executor");
            let executor = executor.ok().expect("created executor");
            let mut reports = Reports::<_>::new(settings, [Id::fixed::<0>()]);
            executor.run(|executor| {
                let result = executor.enter_with(
                    |mut runtime| {
                        let payload = Shared::try_copy(b"x").expect("request payload");
                        let app = Flight::new(payload, canceled, closed, &reason, audit.clone())
                            .closing(ready, shutdown);
                        let setup = session::Setup::new(app, ingress::Plan::none());
                        let wire = wire::Wire::waiting(
                            writes.clone(),
                            entered.clone(),
                            release.clone(),
                        );
                        let hash = runtime.hash_state(health::Domain::DEFAULT);
                        let peers = Snapshot::from_one(
                            Revision::new(1),
                            Endpoint::new(address, address),
                        );
                        let result = plan.bind(domain::Local::new(wire), hash).install::<
                            _, _, sync::Preserve, _, Bundle<Tcp, wire::Wire, profile::Fast>, 1, _, _,
                        >(
                            setup,
                            peers,
                            &mut runtime,
                            Scope::driver,
                            |host, runtime| {
                                let result = runtime.with_quiesce(
                                    &mut finish,
                                    Host::new(queued::Host {
                                        inner: host,
                                        state: fault.clone(),
                                        pending: None,
                                    }),
                                    Duration::from_secs(2),
                                    |mut app| {
                                        let bounded = fibers::Harness::new(Duration::from_millis(500));
                                        assert_eq!(
                                            bounded.try_wait(&mut app, &ready, 1).expect("connect"),
                                            fibers::Wait::Reached,
                                        );
                                        if !shutdown {
                                            assert_eq!(
                                                bounded.try_wait(&mut app, &closed, 1).expect("close"),
                                                fibers::Wait::Reached,
                                                "client retirement lost its send deadline",
                                            );
                                        }
                                    },
                                );
                                assert!(
                                    result.is_ok(),
                                    "client shutdown lost its send deadline: {result:?}, closed={}, entered={}, reason={:?}",
                                    closed.hits(),
                                    entered.hits(),
                                    reason.get(),
                                );
                            },
                        );
                        assert!(result.is_ok(), "client installation");
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
                );
                assert!(result.is_ok(), "executor teardown");
            });
            assert_eq!(release.hits(), 0);
            assert_eq!(closed.hits(), 1);
            assert_eq!(reason.get(), Some(Reason::Local(if shutdown {
                LocalOrigin::RuntimeShutdown
            } else {
                LocalOrigin::ApiRequest
            })));
            if let Some(fault) = fault {
                assert_eq!(fault.queued.hits(), 1);
                assert_eq!(fault.cancelled_flow.get(), Some(batch::Flow::Last));
                let faults = audit.faults.values().expect("receive fault observations");
                assert!(
                    matches!(&*faults, [error::Error::Receive(actual)] if *actual == fault.error),
                    "finalized connection lost its exact receive error",
                );
            } else {
                assert_ne!(entered.hits(), 0, "retirement reaches the held wire");
                checked(&audit);
            }
            assert!(reports.events().expect("reclaimed events").is_empty());
            assert!(writes.values().expect("wire writes").is_empty());
            peer.try_fire().expect("release peer");
        },
    )
    .expect("gated peer");
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}

fn checked(audit: &Report<error::Error, 16>) {
    let faults = audit.faults.values().expect("fault observations");
    let result = error::Error::check(&faults);
    assert!(result.is_ok(), "{result:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
