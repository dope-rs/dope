pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt::Debug,
    io::{self, Read as _, Write as _},
    marker::PhantomData,
    rc::Rc,
};

use dope_core::driver::{
    life::recovery,
    route::{FrameworkTag, KeyTag, Tag, kind},
};
use dope_manifold::{
    app::{env::Bundle, receive},
    timing::Throughput,
};
use dope_net::{link::egress, tcp::Tcp};
use dope_runtime::{executor::session::Completion, topology::error::Kind};
use dope_test::{
    fibers::{Run, Wait},
    harness::{
        peer::Peer,
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};
use o3::buffer::storage;

use crate::common::{
    manifold::listener::terminal::{
        Ingress, OWNED_REPLY, Probe, accepted,
        wire::{self, CleanOnData, CleanOnEof, Event, Wire},
    },
    scenario::case::listener::Listener,
};

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
fn resumes_owned_plaintext() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .direct(0)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<CleanOnEof>, Throughput>>((
            Probe {
                close: close.clone(),
                retired: retired.clone(),
                events: Rc::clone(&events),
                ingress: Ingress::Owned(Some(
                    egress::NonEmpty::try_new(
                        storage::Shared::try_copy(OWNED_REPLY).expect("owned reply"),
                    )
                    .expect("nonempty owned reply"),
                )),
                marker: PhantomData,
                observe: accepted,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports = Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .with(
                        |stream| -> io::Result<_> {
                            stream.write_all(b"x")?;
                            let mut response = [0; OWNED_REPLY.len()];
                            stream.read_exact(&mut response)?;
                            Ok(response)
                        },
                        |peer| {
                            reached(case.try_wait(&close, 1), close.hits(), 1);
                            reached(case.try_wait(&retired, 1), retired.hits(), 1);
                            assert_eq!(
                                &peer.finish().expect("peer finish").expect("peer I/O"),
                                OWNED_REPLY
                            );
                        },
                    )
                    .expect("peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"x".to_vec()),
                Event::Owned(Some(OWNED_REPLY.len())),
                Event::Eof,
                Event::Close,
                Event::Retired,
            ]
    );
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn retires_plaintext() {
    let mut finish = Completion::new();
    let close = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events)));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire<CleanOnData>, Throughput>>((
            Probe {
                close: close.clone(),
                retired: retired.clone(),
                events: events.clone(),
                ingress: Ingress::Reply,
                marker: PhantomData,
                observe: accepted,
            },
            Rc::clone(&wire),
        ))
        .expect("listener layout");
    let mut reports = Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"terminal".to_vec(), |peer| {
                        reached(case.try_wait(&close, 1), close.hits(), 1);
                        reached(case.try_wait(&retired, 1), retired.hits(), 1);
                        assert!(
                            peer.finish()
                                .expect("peer finish")
                                .expect("peer I/O")
                                .is_empty()
                        );
                    })
                    .expect("peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    wire.check();
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert!(
        events.take().as_slice()
            == [
                Event::Chunk(b"terminal".to_vec()),
                Event::Eof,
                Event::Close,
                Event::Retired
            ]
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
