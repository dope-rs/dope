pub mod common;

use std::{convert::Infallible, fmt::Debug};

use dope_core::driver::life::recovery;
use dope_runtime::topology::error::Kind;
use dope_test::fibers::{Run, Wait};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{alloc, cell::Cell, rc::Rc};

use common::{
    manifold::listener::{
        egress::{
            App, Event, RESPONSE,
            apps::{Bounded, BoundedEvent, Direct, DirectWrite},
        },
        pipeline::prefix::Admission,
    },
    scenario::case::{Failure, listener::Listener},
};
use dope_core::driver::route::{FrameworkTag, KeyTag, Tag, kind};
use dope_manifold::{
    BuildError, ConfigError,
    app::{creation, env::Bundle},
    network::listener::{Install, conn::write, handler},
    timing::{Balanced, Throughput},
};
use dope_net::{
    link::egress,
    tcp::Tcp,
    wire::{Identity, domain},
};
use dope_runtime::executor::session::Completion;
use dope_test::{
    alloc::Tracker,
    harness::{
        observe::Trace,
        peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::buffer::storage;

fn observed(events: &[Event], expected: handler::Disposition) {
    assert!(matches!(events, [
        Event::Chunk(peer, input),
        Event::Admitted(Admission::Committed, (0, 0), before, after),
        Event::Released(conn, body, 0, cause),
        Event::Retired(retired, 1, 1),
    ] if input == b"request" && peer == conn && conn == retired && body == RESPONSE && *cause == expected && after + 1 == *before));
}

fn direct(event: DirectWrite<'_>) {
    assert!(matches!(
        event,
        DirectWrite {
            body: Some(_),
            allocations: (0, 0),
            result: Ok(write::Outcome::Committed(()))
        }
    ));
}

fn bounded(event: BoundedEvent<'_>) {
    assert!(match event {
        BoundedEvent::Resident(body) => body == Some(egress::ResidentSize::ZERO),
        BoundedEvent::Write {
            body,
            allocations,
            result,
        } =>
            body.is_some()
                && allocations == (0, 0)
                && matches!(result, Ok(write::Outcome::Committed(()))),
        BoundedEvent::Deferred {
            body,
            called,
            result,
        } =>
            body.is_none()
                && !called
                && matches!(
                    result,
                    Ok(write::Outcome::Deferred(write::Deferred::Capacity))
                ),
        BoundedEvent::Activated { body, result } =>
            !matches!(body, Some(Some(_)))
                && matches!(
                    result,
                    Ok(write::Outcome::Committed(()) | write::Outcome::Deferred(_))
                ),
    });
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
fn owned_reaches_socket() {
    let mut finish = Completion::new();
    for direct in [0, 1] {
        let closed = Gate::new();
        let released = Gate::new();
        let returned = Gate::new();
        let retired = Gate::new();
        let events = Rc::new(Trace::new());
        let queue = egress::Config::shared(egress::Credit::new(
            egress::Entries::new(4),
            egress::Resident::new(64),
        ));
        assert!(
            queue
                .chunks(dope_core::io::transfer::NonZero::fixed_size::<
                    { write::Prefix::CAPACITY },
                >())
                .is_none()
        );
        let prepared = Listener::new::<4>(Default::default())
            .direct(direct)
            .egress(queue)
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
                App {
                    closed: closed.clone(),
                    released: released.clone(),
                    returned: returned.clone(),
                    retired: retired.clone(),
                    abort: false,
                    events: Rc::clone(&events),
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
                    peer::Peer::at(case.addr())
                        .exchange(b"request".to_vec(), |peer| {
                            reached(case.try_wait(&closed, 1), closed.hits(), 1);
                            reached(case.try_wait(&released, 1), released.hits(), 1);
                            reached(case.try_wait(&returned, 1), returned.hits(), 1);
                            reached(case.try_wait(&retired, 1), retired.hits(), 1);
                            let response = peer
                                .finish()
                                .expect("finish client peer")
                                .expect("client peer failed");
                            assert_eq!(response, RESPONSE);
                            assert_eq!(
                                released.hits(),
                                1,
                                "payload owner must retire exactly once"
                            );
                            assert_eq!(returned.hits(), 1, "owner must cross one release callback");
                            assert_eq!(retired.hits(), 1, "retirement audit must run exactly once");
                        })
                        .expect("spawn egress peer");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                native,
            )
            .expect("listener run");
        observed(
            &events.values().expect("egress observations"),
            handler::Disposition::Sent,
        );
        assert!(
            reports.events().expect("reclaimed events").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }
}

#[test]
fn abort_returns_owner() {
    let mut finish = Completion::new();
    let closed = Gate::new();
    let released = Gate::new();
    let returned = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Trace::new());
    let prepared = Listener::new::<4>(Default::default())
        .direct(0)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            App {
                closed: closed.clone(),
                released: released.clone(),
                returned: returned.clone(),
                retired: retired.clone(),
                abort: true,
                events: Rc::clone(&events),
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
                peer::Peer::at(case.addr())
                    .exchange(b"request".to_vec(), |peer| {
                        reached(case.try_wait(&closed, 1), closed.hits(), 1);
                        reached(case.try_wait(&released, 1), released.hits(), 1);
                        reached(case.try_wait(&returned, 1), returned.hits(), 1);
                        reached(case.try_wait(&retired, 1), retired.hits(), 1);
                        assert!(
                            peer.finish()
                                .expect("finish client peer")
                                .expect("client peer failed")
                                .is_empty()
                        );
                        assert_eq!(released.hits(), 1, "payload owner must retire exactly once");
                        assert_eq!(returned.hits(), 1, "owner must cross one release callback");
                        assert_eq!(retired.hits(), 1, "retirement audit must run exactly once");
                    })
                    .expect("spawn egress peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    observed(
        &events.values().expect("egress observations"),
        handler::Disposition::Closed,
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn timing_keeps_layout() {
    assert_eq!(
        alloc::Layout::new::<
            Install<
                'static,
                KeyTag<0>,
                Bundle<Tcp, Identity, Throughput>,
                domain::Local<'static, Identity, KeyTag<0>>,
            >,
        >(),
        alloc::Layout::new::<
            Install<
                'static,
                KeyTag<0>,
                Bundle<Tcp, Identity, Balanced>,
                domain::Local<'static, Identity, KeyTag<0>>,
            >,
        >()
    );
}

#[test]
fn unbounded_uses_fallback() {
    let mut finish = Completion::new();
    let closed = Gate::new();
    let retired = Gate::new();
    let prepared = Listener::new::<1>(Default::default())
        .minimal()
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Direct {
                closed: closed.clone(),
                retired: retired.clone(),
                body: Cell::new(Some(
                    storage::Shared::try_copy(&[0x5a; 2 * 1024 * 1024]).expect("body allocation"),
                )),
                observe: direct,
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
                peer::Peer::at(case.addr())
                    .exchange(b"request".to_vec(), |peer| {
                        reached(case.try_wait(&closed, 1), closed.hits(), 1);
                        reached(case.try_wait(&retired, 1), retired.hits(), 1);
                        let response = peer
                            .finish()
                            .expect("finish client peer")
                            .expect("client peer failed");
                        assert_eq!(response.len(), 1 + 2 * 1024 * 1024);
                        assert_eq!(response[0], 0xa5);
                        assert!(response[1..].iter().all(|byte| *byte == 0x5a));
                    })
                    .expect("spawn egress peer");
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
fn bounded_uses_queue() {
    let mut finish = Completion::new();
    let closed = Gate::new();
    let retired = Gate::new();
    let called = Rc::new(Cell::new(false));
    let prepared = Listener::new::<2>(Default::default())
        .direct(0)
        .egress(egress::Config::shared(egress::Credit::fixed::<
            2,
            { 32 * 1024 },
        >()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Bounded {
                closed: closed.clone(),
                retired: retired.clone(),
                called: called.clone(),
                observe: bounded,
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
                peer::Peer::at(case.addr())
                    .exchange(b"request".to_vec(), |peer| {
                        reached(case.try_wait(&closed, 1), closed.hits(), 1);
                        reached(case.try_wait(&retired, 1), retired.hits(), 1);
                        let response = peer
                            .finish()
                            .expect("finish client peer")
                            .expect("client peer failed");
                        assert_eq!(response[0], 0xb5);
                        assert_eq!(&response[1..response.len() - 1], RESPONSE);
                        assert_eq!(response.last(), Some(&0xc5));
                        assert!(
                            called.get(),
                            "the deferred builder must resume after credit release"
                        );
                    })
                    .expect("spawn egress peer");
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
fn bounded_needs_egress() {
    let mut finish = Completion::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(0)
        .egress(egress::Config::shared(egress::Credit::fixed::<
            1,
            { write::Prefix::CAPACITY as u32 - 1 },
        >()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Bounded {
                closed: Gate::new(),
                retired: Gate::new(),
                called: Rc::new(Cell::new(false)),
                observe: bounded,
            },
            Identity,
        ))
        .expect("bounded listener resource claim");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let entered = Cell::new(false);
    let result = prepared.with_run(
        &mut finish,
        |_| entered.set(true),
        recovery::Policy::new(|event| reports.push(event), recover),
        native,
    );
    assert!(matches!(
        result,
        Err(Failure::Install(BuildError::Create(
            creation::Error::Config(ConfigError::ListenerBoundedEgress { body })
        ))) if body == egress::ResidentSize::ZERO
    ));
    assert!(!entered.get());
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "{reports:?}"
    );
}

#[test]
fn unbounded_needs_direct() {
    let mut finish = Completion::new();
    let prepared = Listener::new::<1>(Default::default())
        .direct(0)
        .egress(egress::Config::shared(egress::Credit::fixed::<
            1,
            { 2 * write::Prefix::CAPACITY as u32 },
        >()))
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Identity, Throughput>>((
            Direct {
                closed: Gate::new(),
                retired: Gate::new(),
                body: Cell::new(None),
                observe: direct,
            },
            Identity,
        ))
        .expect("unbounded listener resource claim");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let entered = Cell::new(false);
    let result = prepared.with_run(
        &mut finish,
        |_| entered.set(true),
        recovery::Policy::new(|event| reports.push(event), recover),
        native,
    );
    assert!(matches!(
        result,
        Err(Failure::Install(BuildError::Create(
            creation::Error::Config(ConfigError::ListenerUnboundedEgress)
        )))
    ));
    assert!(!entered.get());
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "{reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
