pub mod common;

use std::{convert::Infallible, fmt::Debug, num::NonZero, rc::Rc, time::Duration};

use common::{
    manifold::listener::finish::lifetime::{Event, Expiry, Lifetime},
    recover,
    scenario::case::listener::Listener,
};
use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::env::Bundle,
    network::listener::{conn::deadline::Arm, handler},
    timing::Balanced,
};
use dope_net::{link::slot::types::CloseGate, tcp::Tcp, wire::Identity};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers::{Run, Wait},
    harness::{
        observe::Trace,
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::base::time::Positive;

fn deadline(outcome: Arm<'_>) {
    assert!(matches!(outcome, Arm::Armed));
}

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

#[test]
fn persistent() {
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
                expiry: Expiry::Discard,
                drain: handler::Drain::Pending,
                gate: CloseGate::Immediate,
                deadline,
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
                case.try_pause(Duration::from_millis(50))
                    .expect("drive pause");
                assert_eq!(expire.hits(), 0);
                assert_eq!(begun.hits(), 0);
                assert_eq!(closed.hits(), 0);
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

#[test]
fn drain_bound() {
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
                drain: handler::Drain::Pending,
                gate: CloseGate::Deferred,
                deadline,
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
                reached(case.try_wait(&begun, 1), begun.hits(), 1);
                reached(case.try_wait(&closed, 1), closed.hits(), 1);
                assert_eq!(expire.hits(), 0);
                assert_eq!(begun.hits(), 1);
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

#[test]
fn complete_bound() {
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
                drain: handler::Drain::Complete,
                gate: CloseGate::Deferred,
                deadline,
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
                let next = Peer::at(case.addr())
                    .try_connect()
                    .expect("connect next peer");
                reached(case.try_wait(&accept, 2), accept.hits(), 2);
                assert!(matches!(next.peer_addr(), Ok(addr) if addr == case.addr()));
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
fn clear_deadline() {
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
                expiry: Expiry::Clear,
                drain: handler::Drain::Pending,
                gate: CloseGate::Immediate,
                deadline,
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
                reached(case.try_wait(&expire, 1), expire.hits(), 1);
                case.try_pause(Duration::from_millis(50))
                    .expect("drive pause");
                assert_eq!(expire.hits(), 1);
                assert_eq!(begun.hits(), 0);
                assert_eq!(closed.hits(), 0);
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
                deadline,
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
                deadline,
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
                deadline,
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
