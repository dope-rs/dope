use crate::common::manifold::client::error;
pub mod common;

use crate::common::manifold::{client::assert_reclaimed, native};

use std::{cell::Cell, convert::Infallible, mem, net, rc::Rc, time::Duration};

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
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::{
        GUARD,
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
        Flight, Payload, policy, profile,
        reject::{self, Reject},
        wait::{self, PAYLOAD_BYTES, Wait},
        wire::Wire,
    },
    recover,
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
    let listener = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let state = LocalGate::new();
    let closed = state.handle();
    let reason = Cell::new(None);

    let audit = Rc::new(Report::default());

    let planned =
        Connector::<{ reject::MAX }>::new(address, Duration::from_secs(1), Duration::from_secs(1))
            .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, profile::Slow>>(Value::new(Reject {
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
                );
            },
            recovery::Policy::new(|event| reclaim1.push(event), recover),
            native,
        )
        .expect("application scenario");
    assert_reclaimed(&reclaim1);

    audit.no_faults();
    assert_eq!(closed.hits(), 1);
    assert_eq!(
        (audit.sent.get(), audit.cancelled.get()),
        (Some(0), Some(0))
    );
    assert!(matches!(
        reason.get(),
        Some(Reason::Egress(egress::reserve::exact::Failure::Lane { .. }))
    ));
}

#[test]
fn flushes_input_during_kernel_flight() {
    peer::AcceptScript::with(
        |listener| {
            let stream = listener.accept().expect("scripted peer");
            let mut byte = [0; 1];
            assert_eq!(stream.peek(&mut byte).expect("native send began"), 1);
            stream.write_all(b"x").expect("write blocked input");
            assert!(matches!(
                listener.accept_for(GUARD),
                Err(peer::Error::Cancelled)
            ));
        },
        |peer| {
            let mut finish = Completion::new();
            let cancel = LocalGate::new();
            let canceled = cancel.handle();
            let state = LocalGate::new();
            let closed = state.handle();
            let input = LocalGate::new();
            let flushed = input.handle();
            let reason = Cell::new(None);
            let payload = (storage::Shared::try_from(&mut vec![0; PAYLOAD_BYTES]))
                .expect("owned payload retains its allocation");

            let audit = Rc::new(Report::default());

            let planned = Connector::<{ wait::MAX }>::new(
                peer.addr(),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .egress(egress::Config::shared(egress::Credit::new(
                egress::Entries::new(4),
                egress::Resident::new(PAYLOAD_BYTES as u32),
            )))
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, profile::Slow>>(Value::new(
                Flight::new(payload, canceled, closed, &reason, audit.clone()).blocked(flushed),
            ))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&flushed, 1).expect("input flush wait"),
                            fibers::Wait::Reached
                        );
                        assert_eq!(canceled.hits(), 0);
                        assert_eq!(closed.hits(), 0);
                        assert_eq!(
                            case.try_wait(&closed, 1).expect("progress wait"),
                            fibers::Wait::Reached
                        )
                    },
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    native,
                )
                .expect("application scenario");
            assert_reclaimed(&reclaim1);

            audit.no_faults();
            assert_eq!(canceled.hits(), 1);
            assert_eq!(closed.hits(), 1);
            assert!((reason.get()) == (Some(Reason::Timeout(deadline::Kind::Send))));
            peer.stop().expect("scripted peer");
        },
    )
    .expect("scripted peer");
}

#[test]
fn starts_before_flight() {
    let mut finish = Completion::new();
    let listener = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let cancel = LocalGate::new();
    let canceled = cancel.handle();
    let state = LocalGate::new();
    let closed = state.handle();
    let reason = Cell::new(None);
    let payload = (Shared::try_copy(b"x")).expect("allocate one-byte payload");

    let audit = Rc::new(Report::default());

    let planned =
        Connector::<{ wait::MAX }>::new(address, Duration::from_secs(1), Duration::from_secs(1))
            .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Fast>>(Value::new(Flight::new(
                payload,
                canceled,
                closed,
                &reason,
                audit.clone(),
            )))
            .expect("prepare actual client layout");
    let mut reclaim2 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            |case| {
                assert_eq!(
                    case.try_wait(&closed, 1).expect("progress wait"),
                    fibers::Wait::Reached
                );
            },
            recovery::Policy::new(|event| reclaim2.push(event), recover),
            native,
        )
        .expect("application scenario");
    assert_reclaimed(&reclaim2);

    audit.no_faults();
    assert_eq!(canceled.hits(), 1);
    assert_eq!(closed.hits(), 1);
    assert!((reason.get()) == (Some(Reason::Timeout(deadline::Kind::Send))));
}

#[test]
fn starts_on_capacity_wait() {
    let mut finish = Completion::new();
    let listener = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let mut held = [const { None }; 2];
    let index = Rc::new(Cell::new(None));
    let reasons = Rc::new(Trace::new());
    let connected = Gate::new();
    let order = Rc::new(Trace::new());
    let negotiated = Gate::new();
    let seen = Gate::new();
    let closed = Gate::new();

    let audit = Rc::new(Report::default());

    let planned = Connector::<2>::new(address, Duration::from_secs(1), Duration::from_secs(1))
        .egress(egress::Config::shared(egress::Credit::fixed::<1, 1>()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, profile::Fast>>(Value::new(Wait {
            audit: audit.clone(),

            pending: [const { Cell::new(None) }; 2],
            index: index.clone(),
            reasons: reasons.clone(),
            connected: connected.clone(),
            order: order.clone(),
            negotiated: negotiated.clone(),
            seen: seen.clone(),
            closed: closed.clone(),
        }))
        .expect("prepare actual client layout");
    let mut reclaim3 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

    planned
        .with_run(
            &mut finish,
            |case| {
                for index in 0..held.len() {
                    assert_eq!(
                        case.try_wait(&connected, index as u32 + 1)
                            .expect("connect"),
                        fibers::Wait::Reached
                    );
                    let id = order.values().expect("connected order")[index];
                    held[id] = Some(
                        listener
                            .accept_for(GUARD)
                            .expect("accept")
                            .expect("connected peer"),
                    );
                }
                assert_eq!(
                    case.try_wait(&seen, 1).expect("progress wait"),
                    fibers::Wait::Reached
                );
                let waiting = index.get().expect("waiting connection");
                held[waiting]
                    .as_ref()
                    .expect("waiting peer")
                    .write_all(b"negotiate")
                    .expect("negotiate while credit is denied");
                assert_eq!(
                    case.try_wait(&negotiated, 1).expect("negotiation progress"),
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
    assert_reclaimed(&reclaim3);

    audit.no_faults();
    let reasons = reasons.values().expect("disconnect reasons");
    assert!((1..=2).contains(&reasons.len()));
    let waiting = (index.get()).expect("one waiting source");
    assert!(reasons.contains(&(waiting, Reason::Timeout(deadline::Kind::Send))));
    drop(held);
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

#[test]
fn bound_consumes_transmit_before_quarantine() {
    policy::check_transmit::<true, true>();
}

#[test]
fn bound_default_retains_transmit() {
    policy::check_transmit::<false, false>();
}

#[test]
fn bound_retains_transmit_once_before_quarantine() {
    policy::check_transmit::<false, true>();
}

fn check_retirement(shutdown: bool, fault: Option<Errno>) {
    use dope_core::io::event::batch;
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
    use dope_runtime::executor::session::Scope;

    use crate::common::{
        manifold::{client::switch::terminal::queued, listener::finish::close::wire},
        scenario::Runtime,
    };

    let listener = peer::TcpAcceptor::bind((net::Ipv4Addr::LOCALHOST, 0).into()).expect("listener");
    let address = listener.local_addr().expect("listener address");
    let mut finish = Completion::new();
    let ready = LocalGate::new();
    let ready = ready.handle();
    let quarantined = LocalGate::new();
    let quarantined = quarantined.handle();
    let canceled = LocalGate::new();
    let canceled = canceled.handle();
    let closed = LocalGate::new();
    let closed = closed.handle();
    let entered = Gate::new();
    let release = Gate::new();
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
        plan::Spec::new(Duration::from_secs(1), Duration::from_secs(1)).expect("retry bounds"),
    )
    .expect("client layout");
    let runtime = Runtime::try_for_plan::<profile::Fast, _>(&plan).expect("client settings");
    let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);
    runtime.with_scope(
        |mut runtime| {
            let payload = Shared::try_copy(b"x").expect("request payload");
            let app = Flight::new(payload, canceled, closed, &reason, audit.clone())
                .closing(ready, quarantined, shutdown);
            let setup = session::Setup::new(app, ingress::Plan::none());
            let wire = wire::Wire::waiting(entered.clone(), release.clone());
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
                            renewal: None,
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
    assert_eq!(release.hits(), 0);
    assert_eq!(quarantined.hits(), 1, "late close must preserve retirement");
    assert_eq!(closed.hits(), 1);
    assert_eq!(
        reason.get(),
        Some(Reason::Local(if shutdown {
            LocalOrigin::RuntimeShutdown
        } else {
            LocalOrigin::ApiRequest
        }))
    );
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
        audit.no_faults();
    }
    assert!(reports.events().expect("reclaimed events").is_empty());
}
