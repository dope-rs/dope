pub mod common;

use std::{num::NonZero, rc::Rc, time::Duration};

use common::manifold::listener::{native, reached};
use common::{
    manifold::listener::finish::lifetime::{Event, Expiry, Lifetime},
    recover,
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{app::env::Bundle, network::listener::handler, timing::Balanced};
use dope_net::{link::slot::types::CloseGate, tcp::Tcp, wire::Identity};
use dope_runtime::executor::session::Completion;
use dope_test::{
    harness::{
        observe::Trace,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::base::time::Positive;

#[test]
fn persistent_and_clear_deadline() {
    for (expiry, count) in [
        (Expiry::Discard, 0),
        (Expiry::Clear, 1),
        (Expiry::Overflow, 1),
        (Expiry::Disabled, 1),
    ] {
        let closes = u32::from(matches!(expiry, Expiry::Overflow | Expiry::Disabled));
        let mut finish = Completion::new();
        let accept = Gate::new();
        let expire = Gate::new();
        let begun = Gate::new();
        let closed = Gate::new();
        let prepared = Listener::new::<1>(Default::default())
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>((
                Lifetime {
                    accept: accept.clone(),
                    expire: expire.clone(),
                    begun: begun.clone(),
                    closed: closed.clone(),
                    expiry,
                    drain: handler::Drain::Pending,
                    gate: CloseGate::Immediate,
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
                    let peer = Peer::at(case.addr())
                        .try_connect()
                        .expect("connect lifetime peer");
                    reached(case.try_wait(&accept, 1), accept.hits(), 1);
                    if count != 0 {
                        reached(case.try_wait(&expire, count), expire.hits(), count);
                    }
                    if closes != 0 {
                        reached(case.try_wait(&closed, closes), closed.hits(), closes);
                    } else {
                        case.try_pause(Duration::from_millis(50))
                            .expect("drive pause");
                    }
                    assert_eq!(expire.hits(), count);
                    assert_eq!(begun.hits(), 0);
                    assert_eq!(closed.hits(), closes);
                    assert!(matches!(peer.peer_addr(), Ok(addr) if addr == case.addr()));
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
}

#[test]
fn drain_bound() {
    for drain in [handler::Drain::Pending, handler::Drain::Complete] {
        let mut finish = Completion::new();
        let accept = Gate::new();
        let expire = Gate::new();
        let begun = Gate::new();
        let closed = Gate::new();
        let prepared = Listener::new::<1>(Default::default())
            .lifetime(handler::Lifetime::graceful(
                Positive::from_millis::<20>(),
                Positive::from_millis::<20>(),
            ))
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>((
                Lifetime {
                    accept: accept.clone(),
                    expire: expire.clone(),
                    begun: begun.clone(),
                    closed: closed.clone(),
                    expiry: Expiry::Discard,
                    drain,
                    gate: CloseGate::Deferred,
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
                    {
                        let peer = Peer::at(case.addr())
                            .try_connect()
                            .expect("connect lifetime peer");
                        reached(case.try_wait(&accept, 1), accept.hits(), 1);
                        reached(case.try_wait(&begun, 1), begun.hits(), 1);
                        reached(case.try_wait(&closed, 1), closed.hits(), 1);
                        assert_eq!(expire.hits(), 0);
                        assert_eq!(begun.hits(), 1);
                        assert!(matches!(peer.peer_addr(), Ok(addr) if addr == case.addr()));
                    }
                    if matches!(drain, handler::Drain::Complete) {
                        let next = Peer::at(case.addr())
                            .try_connect()
                            .expect("connect next peer");
                        reached(case.try_wait(&accept, 2), accept.hits(), 2);
                        assert!(matches!(next.peer_addr(), Ok(addr) if addr == case.addr()));
                    }
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
}

#[test]
fn past_application_deadline_preserves_own_lifetime_and_drain() {
    let mut finish = Completion::new();
    let accept = Gate::new();
    let expire = Gate::new();
    let begun = Gate::new();
    let closed = Gate::new();
    let events = Rc::new(Trace::new());
    let prepared = Listener::new::<1>(Default::default())
        .lifetime(handler::Lifetime::graceful(
            Positive::from_millis::<20>(),
            Positive::from_millis::<20>(),
        ))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>((
            Lifetime {
                accept: accept.clone(),
                expire: expire.clone(),
                begun,
                closed: closed.clone(),
                expiry: Expiry::Rearm {
                    slot: 0,
                    count: None,
                    events: events.clone(),
                },
                drain: handler::Drain::Pending,
                gate: CloseGate::Deferred,
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
                let _peer = Peer::at(case.addr())
                    .try_connect()
                    .expect("connect repeating deadline peer");
                reached(case.try_wait(&accept, 1), accept.hits(), 1);
                reached(case.try_wait(&closed, 1), closed.hits(), 1);
                assert!(expire.hits() > 1, "application deadline was repeatedly due");
                assert_eq!(
                    &*events.values().expect("lifetime events"),
                    &[Event::Drain(0), Event::Close(0)]
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(reports.events().expect("reclaimed events").is_empty());
}

#[test]
fn past_application_deadline_preserves_other_connection_bounds() {
    let mut finish = Completion::new();
    let accept = Gate::new();
    let expire = Gate::new();
    let begun = Gate::new();
    let closed = Gate::new();
    let events = Rc::new(Trace::new());
    let prepared = Listener::new::<2>(Default::default())
        .lifetime(handler::Lifetime::graceful(
            Positive::from_millis::<100>(),
            Positive::from_millis::<20>(),
        ))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>((
            Lifetime {
                accept: accept.clone(),
                expire: expire.clone(),
                begun: begun.clone(),
                closed: closed.clone(),
                expiry: Expiry::Rearm {
                    slot: 1,
                    count: None,
                    events: events.clone(),
                },
                drain: handler::Drain::Pending,
                gate: CloseGate::Deferred,
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
                let _first = Peer::at(case.addr())
                    .try_connect()
                    .expect("connect earlier lifetime peer");
                reached(case.try_wait(&accept, 1), accept.hits(), 1);
                let _second = Peer::at(case.addr())
                    .try_connect()
                    .expect("connect repeating deadline peer");
                reached(case.try_wait(&accept, 2), accept.hits(), 2);
                assert_eq!(begun.hits(), 0, "both peers precede the lifetime bounds");
                reached(case.try_wait(&closed, 2), closed.hits(), 2);
                assert!(expire.hits() > 1, "application deadline was repeatedly due");
                let events = events.values().expect("lifetime events");
                let position = |event| {
                    events
                        .iter()
                        .position(|actual| *actual == event)
                        .expect("connection event")
                };
                assert!(position(Event::Drain(0)) < position(Event::Drain(1)));
                assert!(position(Event::Close(0)) < position(Event::Close(1)));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(reports.events().expect("reclaimed events").is_empty());
}

#[test]
fn overdue_application_backlog_remains_admitted() {
    let mut finish = Completion::new();
    let accept = Gate::new();
    let expire = Gate::new();
    let begun = Gate::new();
    let closed = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Balanced>>((
            Lifetime {
                accept: accept.clone(),
                expire: expire.clone(),
                begun,
                closed: closed.clone(),
                expiry: Expiry::Rearm {
                    slot: 0,
                    count: NonZero::new(3),
                    events: Rc::new(Trace::new()),
                },
                drain: handler::Drain::Pending,
                gate: CloseGate::Deferred,
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
                let _peer = Peer::at(case.addr())
                    .try_connect()
                    .expect("connect overdue backlog peer");
                reached(case.try_wait(&accept, 1), accept.hits(), 1);
                reached(case.try_wait(&expire, 3), expire.hits(), 3);
                case.try_pause(Duration::from_millis(20))
                    .expect("drain application backlog");
                assert_eq!(expire.hits(), 3);
                assert_eq!(closed.hits(), 0, "overdue application work remains valid");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(reports.events().expect("reclaimed events").is_empty());
}

#[test]
fn retained_native_input_cannot_cancel_expired_application_deadline() {
    use std::cell::Cell;

    use common::{
        manifold::listener::{
            defer::script::Until, finish::lifetime::DeadlineAdmission, trace::One,
        },
        observed::{Observed, State},
    };
    use dope_core::driver::life::creation::Attempt;
    use dope_manifold::{app::Host, network::listener, timing::Throughput};
    use dope_net::wire::domain;
    use dope_runtime::executor::{Executor, session};

    for activation in [false, true] {
        let mut finish = Completion::new();
        let completed = Gate::new();
        let expired = Gate::new();
        let observed = Rc::new(State::default());
        observed.hold.set(true);
        let activate = Rc::new(Cell::new(false));
        let settings = One::driver().expect("listener layout");
        let mut attempt = Attempt::new();
        let executor = Executor::new(&mut attempt, settings);
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("listener executor");
        let executor = executor.ok().expect("created executor");
        let mut reports = Reports::<_>::new(settings, [<FrameworkTag<kind::Any> as Tag>::ROUTE]);

        executor.run(|executor| {
            let result = executor.enter_with(
                |mut session| {
                    let target = Rc::new(Cell::new(None));
                    let hash = session.hash_state(listener::Domain::DEFAULT);
                    listener::Install::<KeyTag<0>, Bundle<Tcp, Identity, Throughput>, _>::new(
                        One::config(),
                        domain::Local::new(Identity),
                    )
                    .with_driver(
                        DeadlineAdmission {
                            target: Rc::clone(&target),
                            activate: Rc::clone(&activate),
                            completed: completed.clone(),
                            expired: expired.clone(),
                        },
                        hash,
                        &mut session,
                        session::Scope::driver,
                        |listener, _wait, session| {
                            let addr = listener.local_addr();
                            let start = session.driver().driver_ref().clock().now().into();
                            let expired = session
                                .with_at_for(
                                    &mut finish,
                                    Host::new(Observed {
                                        listener,
                                        state: Rc::clone(&observed),
                                        pending: None,
                                    }),
                                    start,
                                    Duration::from_secs(2),
                                    |mut app| {
                                        let peer = Peer::at(addr).try_connect().expect("connect peer");
                                        peer.write_all(b"A").expect("write native input");
                                        let held = app.drive_for(
                                            start,
                                            Duration::from_secs(1),
                                            Until {
                                                gate: &observed.intercepted,
                                                hits: 1,
                                            },
                                        )
                                        .map(|done| done.into_output());
                                        assert!(held.is_ok(), "retain native completion: {held:?}; accepted={}, expired={}, completed={}, intercepted={}, parked={}", target.get().is_some(), expired.hits(), completed.hits(), observed.intercepted.hits(), observed.parked.get());
                                        assert_eq!(completed.hits(), 0);
                                        if activation {
                                            activate.set(true);
                                            target.get().expect("accepted target").wake();
                                        } else {
                                            observed.hold.set(false);
                                        }
                                        let due = start
                                            .try_add(Duration::from_millis(11))
                                            .expect("expired deadline coordinate");
                                        app.drive_for(
                                            due,
                                            Duration::from_secs(1),
                                            Until {
                                                gate: &completed,
                                                hits: 1,
                                            },
                                        )
                                        .expect("resolve expired admission")
                                        .into_output();
                                        let count = expired.hits();
                                        observed.hold.set(false);
                                        activate.set(false);
                                        app.drive_for(
                                            due,
                                            Duration::from_secs(1),
                                            Until {
                                                gate: &completed,
                                                hits: 1,
                                            },
                                        )
                                        .expect("settle held native input")
                                        .into_output();
                                        assert!(app.inspect(
                                            |host| host.manifold(),
                                            |host| host.get_ref().pending.is_none(),
                                        ));
                                        count
                                    },
                                )
                                .expect("listener teardown");
                            assert_eq!(
                                expired, 1,
                                "application callback cancelled a due deadline; activation={activation}",
                            );
                        },
                    )
                    .expect("listener installation");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            );
            assert!(result.is_ok(), "listener session");
        });
        assert!(reports.events().expect("reclaimed events").is_empty());
    }
}
