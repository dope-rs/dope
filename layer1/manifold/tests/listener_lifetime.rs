pub mod common;

use std::{convert::Infallible, fmt::Debug, time::Duration};

use common::{
    manifold::listener::finish::lifetime::{Expiry, Lifetime},
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
