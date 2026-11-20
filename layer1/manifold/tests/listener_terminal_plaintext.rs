pub mod common;

use common::manifold::listener::reached;

use std::{cell::Cell, marker::PhantomData, rc::Rc};

use dope_core::driver::route::KeyTag;
use dope_manifold::{
    app::{env::Bundle, receive},
    timing::Throughput,
};
use dope_net::{link::egress, tcp::Tcp};
use dope_test::harness::{
    peer::{self, Peer},
    signals::{Gate, Progress as _},
};
use o3::buffer::storage;

use crate::common::{
    manifold::listener::terminal::{
        Ingress, OWNED_REPLY, Probe, accepted,
        wire::{self, Event, Mode, Wire},
    },
    scenario::case::listener::Listener,
};

#[test]
fn resumes_owned_plaintext() {
    let close = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(
        Rc::clone(&events),
        Mode::CleanOnEof { control: false },
    ));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .direct(0)
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
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
    prepared.run(|case| {
        Peer::at(case.addr())
            .with(
                |stream| -> Result<_, peer::Error> {
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
    });
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
}

#[test]
fn retires_plaintext() {
    let close = Gate::new();
    let retired = Gate::new();
    let events = Rc::new(Cell::new(Vec::new()));
    let wire = Rc::new(wire::Probe::new(Rc::clone(&events), Mode::CleanOnData));
    let prepared = Listener::<receive::Coupled>::new::<1>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
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
    prepared.run(|case| {
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
    });
    wire.check();
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
