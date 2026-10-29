pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt::Debug,
    net,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{
        life::recovery,
        route::{Id, KeyTag},
    },
    io::socket,
};
use dope_manifold::{
    app::env::Bundle,
    client::{close::Reason, deadline},
    timing::{Balanced, Throughput},
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers,
    fibers::Wait,
    harness::{
        observe::Trace,
        peer::{self, TcpScript},
        signals::{Gate, LocalGate, Progress as _},
    },
    reclaim::Reports,
};
use o3::base::time::Positive;

use crate::common::{
    contract::Report,
    manifold::client::{
        finish::{
            lifetime::{self, LIMIT, Lifetime, Profile},
            shutdown::{CONNECTIONS, Shutdown},
        },
        probe::{self, IDLE_TIMEOUT, Probe, REDIAL_BACKOFF},
    },
    scenario::case::{Connector, Value},
};

#[test]
fn age_timeout_is_typed() {
    let mut finish = Completion::new();

    TcpScript::with_many::<2, _>(
        |_| {},
        |server| {
            let addr = server.addr();
            let ready_g = LocalGate::new();
            let ready = ready_g.handle();
            let events = Trace::<lifetime::Event, 16>::new();
            let audit = Rc::new(Report::default());

            let planned = Connector::<{ lifetime::MAX }>::new(
                addr,
                Duration::from_millis(20),
                Duration::from_millis(200),
            )
            .lifetime(LIMIT)
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Profile>>(Value::new(Lifetime {
                audit: audit.clone(),

                ready,
                events: &events,
            }))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&ready, 2).expect("progress wait"),
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

            let finish = server.finish();
            assert!(finish.is_ok(), "finish held peers");
            audit.checked((0, 0));
            assert_eq!(ready.hits(), 2);
            let events = events.values().expect("lifetime lifecycle observations");
            assert!(
                matches!(events.as_ref(), [
                    lifetime::Event::Connected(Some(first)),
                    lifetime::Event::Quarantined(1),
                    lifetime::Event::Closed(Reason::Timeout(deadline::Kind::Lifetime)),
                    lifetime::Event::Connected(Some(second)),
                    lifetime::Event::Quarantined(2),
                    lifetime::Event::Closed(_),
                ] if first.is_none_or(|left| left <= LIMIT.get()) && second.is_none_or(|left| left <= LIMIT.get())),
                "{events:?}"
            );
        },
    )
    .expect("spawn held peers");
}

#[test]
fn drains_over_turn_limit() {
    let mut finish = Completion::new();
    peer::TcpScript::try_gated::<CONNECTIONS>(
        |_| {},
        |address, release| {
            let ready = Gate::new();
            let closed = Gate::new();
            let audit = Rc::new(Report::default());

            let planned = Connector::<CONNECTIONS>::new(
                address,
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>(Value::new(Shutdown {
                audit: audit.clone(),

                connected: ready.clone(),
                disconnected: closed.clone(),
            }))
            .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            planned
                .with_run(
                    &mut finish,
                    |case| {
                        assert_eq!(
                            case.try_wait(&ready, CONNECTIONS as u32)
                                .expect("progress wait"),
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

            assert_eq!(closed.hits(), CONNECTIONS as u32);
            assert_eq!(ready.hits(), CONNECTIONS as u32);
            audit.checked((0, 0));
            release.try_fire().expect("release scripted peers");
        },
    )
    .expect("gated peers");
}

#[test]
fn idle_redials() {
    let (reached, hits, redial) = run(Some(IDLE_TIMEOUT), 2);
    assert!((reached) == (Wait::Reached));
    assert_eq!(hits, 2);
    let dt = (redial).expect("must reconnect after silence");
    assert!(dt >= IDLE_TIMEOUT.get() - Duration::from_millis(30));
    assert!(
        dt < IDLE_TIMEOUT.get() * 8,
        "reconnect at {dt:?} — far past the deadline; watchdog is late"
    );
}

#[test]
fn quiescent_stays() {
    let (reached, gate, redial) = run(None, 2);
    assert!((reached) == (Wait::Deadline));
    assert_eq!(gate, 1);
    assert_eq!(redial, None);
}

fn run(timeout: Option<Positive>, want: u32) -> (Wait, u32, Option<Duration>) {
    let mut finish = Completion::new();

    TcpScript::with_many::<2, _>(
        |_| {},
        |server| {
            let addr = server.addr();
            let gate_g = LocalGate::new();
            let gate = gate_g.handle();
            let redial = Cell::new(None);
            let audit = Rc::new(Report::default());

            let planned = Connector::<{ probe::MAX }>::new(addr, REDIAL_BACKOFF, REDIAL_BACKOFF)
                .prepare_off::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>(Value::new(Probe {
                    audit: audit.clone(),

                    timeout,
                    peer: move |peer: socket::Addr| {
                        assert_eq!(peer.into_std().expect("TCP peer address"), addr)
                    },
                    gate,
                    start: Instant::now(),
                    redial: &redial,
                }))
                .expect("prepare actual client layout");
            let mut reclaim1 = Reports::<_>::new(planned.layout(), [Id::fixed::<0>()]);

            let reached = planned
                .with_run(
                    &mut finish,
                    |case| case.try_wait(&gate, want),
                    recovery::Policy::new(|event| reclaim1.push(event), recover),
                    native,
                )
                .expect("liveness scenario")
                .expect("wait for liveness scenario");
            assert!(
                matches!(reclaim1.events(), Ok(events) if events.is_empty()),
                "unexpected reclaimed operations: {reclaim1:?}"
            );

            if matches!(reached, Wait::Deadline) {
                net::TcpStream::connect(addr).expect("release liveness peer");
            }
            server.finish().expect("finish liveness peer");
            audit.checked((0, 0));
            (reached, gate.hits(), redial.get())
        },
    )
    .expect("spawn liveness peer")
}

fn native<S: Debug, T: Debug>(result: Result<T, Kind<'_, S, &T>>) -> T {
    result.expect("native lifecycle")
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
