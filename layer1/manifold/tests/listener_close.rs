pub mod common;

use std::{net::Shutdown, rc::Rc};

use common::manifold::{listener::reached, native};
use common::{
    manifold::{
        Pattern,
        listener::finish::close::{
            control,
            duplex::{Duplex, Fence, Flow, REPLY},
            script,
            wire::{BYE, Wire},
        },
    },
    recover,
    scenario::case::listener::Listener,
};
use dope_core::{
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
    },
    io::socket,
};
use dope_manifold::{app::env::Bundle, timing::Throughput};
use dope_net::{
    tcp::Tcp,
    wire::receive::batch::{self, Spare},
};
use dope_runtime::executor::session::{self, Completion};
use dope_test::{
    harness::{
        observe::Trace,
        peer::{self, Peer},
        scenario::run::{self, Task as _},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
};

#[test]
fn flushes_after_plaintext() {
    let mut finish = session::Completion::new();
    assert!(batch::Capacity::<control::Control>::fit_spare(Spare::from_usize(0)).is_none());
    let capacity = batch::Capacity::<control::Control>::fit_spare(Spare::from_usize(1))
        .expect("pair capacity");
    assert_eq!(capacity.items().get(), 2);
    let gate = Gate::new();
    let seen = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, control::Control, Throughput>>((
            control::App {
                close: gate.clone(),
                received: seen.clone(),
            },
            (),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .exchange(b"REQ".to_vec(), |peer| {
                        reached(case.try_wait(&gate, 1), gate.hits(), 1);
                        assert_eq!(
                            peer.finish()
                                .expect("finish client peer")
                                .expect("client peer failed"),
                            control::OUTPUT
                        );
                        assert_eq!(&*seen.values().expect("control observations"), b"REQ");
                        assert_eq!(gate.hits(), 1);
                    })
                    .expect("peer spawn failed");
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
fn fence_drains_first() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Fence {
                close: gate.clone(),
            },
            Wire::default(),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .with(
                        |stream| peer::Half::try_run(stream, b"REQ"),
                        |peer| {
                            reached(case.try_wait(&gate, 1), gate.hits(), 1);
                            let mut expected = REPLY.to_vec();
                            expected.extend_from_slice(BYE);
                            assert_eq!(
                                peer.finish()
                                    .expect("finish client peer")
                                    .expect("client peer failed"),
                                expected
                            );
                        },
                    )
                    .expect("peer spawn failed");
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
fn write_half_keeps_receive() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Duplex {
                close: gate.clone(),
            },
            Wire::default(),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    prepared
        .with_run(
            &mut finish,
            |case| {
                Peer::at(case.addr())
                    .with(
                        |stream| -> Result<Vec<u8>, Flow> {
                            stream.write_all(b"FIRST").map_err(Flow::First)?;
                            let mut response = Vec::new();
                            stream.read_to_end(&mut response).map_err(Flow::Read)?;
                            stream.write_all(b"SECOND").map_err(Flow::Second)?;
                            stream.shutdown(Shutdown::Write).map_err(Flow::Shutdown)?;
                            Ok(response)
                        },
                        |peer| {
                            reached(case.try_wait(&gate, 1), gate.hits(), 1);
                            let mut expected = REPLY.to_vec();
                            expected.extend_from_slice(BYE);
                            let response = peer.finish().expect("peer finish").expect("peer I/O");
                            assert_eq!(response, expected);
                            assert_eq!(gate.hits(), 1);
                        },
                    )
                    .expect("peer spawn failed");
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
fn drains_reply_and_peer_eof() {
    for (payload, request, eof) in [
        (Some(Pattern(12_000).bytes()), b"GET\n".as_slice(), false),
        (None, b"REQ".as_slice(), true),
    ] {
        let mut finish = Completion::new();
        let gate = Gate::new();
        let prepared = Listener::new::<64>(Default::default())
            .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
                script::App {
                    payload: payload.clone(),
                    close: gate.clone(),
                    chunk: None,
                    timeout: None,
                },
                Wire::default(),
            ))
            .expect("listener layout");
        let mut reports =
            Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        prepared
            .with_run(
                &mut finish,
                |case| {
                    Peer::at(case.addr())
                        .with(
                            |stream| -> Result<Vec<u8>, peer::Error> {
                                stream.write_all(request)?;
                                if eof {
                                    stream.shutdown(Shutdown::Write)?;
                                }
                                Peer::try_read_all(stream)
                            },
                            |peer| {
                                reached(case.try_wait(&gate, 1), gate.hits(), 1);
                                let got = peer
                                    .finish()
                                    .expect("drain peer finish")
                                    .expect("drain peer I/O");
                                let mut expected = payload.unwrap_or_default();
                                expected.extend_from_slice(BYE);
                                assert_eq!(got, expected);
                                assert_eq!(gate.hits(), 1);
                            },
                        )
                        .expect("spawn drain peer");
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
fn quiesces_buffered_send() {
    let mut finish = Completion::new();
    let chunk = Gate::new();
    let close = Gate::new();
    let bound = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener bind");
    let addr = bound.local_addr();
    let mut spec = Listener::new::<64>(Default::default());
    spec.bound = Some(bound);
    let prepared = spec
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            script::App {
                payload: None,
                close: close.clone(),
                chunk: Some(chunk.clone()),
                timeout: None,
            },
            Wire::default(),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let (progress, got) = run::Entry(move || script::Script(addr).try_read())
        .with(|| {
            prepared
                .with_run(
                    &mut finish,
                    |case| case.try_wait(&chunk, 1),
                    recovery::Policy::new(|event| reports.push(event), recover),
                    native,
                )
                .expect("listener run")
        })
        .expect("buffered peer start");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    let got = got.expect("buffered peer read");
    reached(progress, chunk.hits(), 1);
    assert_eq!(got, BYE);
    assert_eq!(close.hits(), 1);
}
