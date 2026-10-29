pub mod common;

use std::{
    convert::Infallible,
    fmt::Debug,
    io::{self, Read as _, Write as _},
    net::Shutdown,
    rc::Rc,
};

use common::{
    manifold::{
        Pattern,
        listener::finish::close::{
            control,
            duplex::{self, Duplex, Fence, Flow, REPLY},
            script,
            wire::{BYE, Wire},
        },
    },
    scenario::case::listener::Listener,
};
use dope_core::{
    dope_sys::io::socket::record::Record,
    driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
    },
    platform::channel::Channel,
};
use dope_manifold::{app::env::Bundle, network::listener::conn::write, timing::Throughput};
use dope_net::{
    tcp::Tcp,
    wire::receive::batch::{self, Spare},
};
use dope_runtime::{
    executor::session::{self, Completion},
    topology::error::Kind,
};
use dope_test::{
    fibers::{Run, Wait},
    harness::{
        observe::Trace,
        peer::{self, Peer},
        scenario::run::{self, Task as _},
        signals::{Gate, Progress as _},
    },
    reclaim::Reports,
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

fn control(event: control::Event<'_, '_>) {
    match event {
        control::Event::Close(state) => assert!(matches!(state, control::State::Seen(_))),
        control::Event::Send(state, now) => {
            assert!(matches!(state, control::State::Seen(input) if *input <= now));
        }
    }
}

fn fence(event: duplex::Event<'_>) {
    use duplex::{Event, Phase};
    match event {
        Event::Eof(state) | Event::Close(state) => assert!(matches!(state, Phase::Seen(_))),
        Event::Prefix(state) => assert!(matches!(state, Phase::Held(_))),
        Event::Write(result) => assert!(matches!(result, Ok(write::Outcome::Committed(())))),
        Event::Chunk(len, state) => {
            assert!(len != 0 && matches!(state, Phase::Fresh | Phase::Seen(_)));
        }
        Event::Sent(state, now) => assert!(matches!(state, Phase::Held(input) if input <= now)),
    }
}

fn duplex(event: duplex::Half<'_>) {
    use duplex::{Half, Stage};
    match event {
        Half::Close(state) | Half::Prefix(state) => assert!(matches!(state, Stage::Seen)),
        Half::Chunk(len) => assert_ne!(len, 0),
        Half::Write(result) => assert!(matches!(result, Ok(write::Outcome::Committed(())))),
        Half::SealedPrefix(state, len) => assert!(matches!(state, Stage::Seen) && len != 0),
        Half::Sealed(result) => assert!(matches!(result, Ok(write::Outcome::Closed))),
    }
}

fn script(event: script::Event<'_>) {
    match event {
        script::Event::Close(state) => assert!(state <= 1),
        script::Event::Chunk(len) => assert_ne!(len, 0),
        script::Event::Prefix(state) => assert_eq!(state, 0),
        script::Event::Write(result) => {
            assert!(matches!(result, Ok(write::Outcome::Committed(()))))
        }
    }
}

#[test]
fn flushes_after_plaintext() {
    let mut finish = session::Completion::new();
    assert!(batch::Capacity::<control::Control>::fit_spare(Spare::from_usize(0)).is_none());
    let capacity = batch::Capacity::<control::Control>::fit_spare(Spare::from_usize(1))
        .expect("pair capacity");
    assert_eq!(capacity.items().get(), 2);
    let gate = Gate::new();
    let seen = Rc::new(Trace::new());
    let writes = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, control::Control, Throughput>>((
            control::App {
                close: gate.clone(),
                received: seen.clone(),
                observe: control,
            },
            Rc::clone(&writes),
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
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

#[test]
fn fence_drains_first() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let writes = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Fence {
                close: gate.clone(),
                observe: fence,
            },
            Rc::clone(&writes),
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
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

#[test]
fn write_half_keeps_receive() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let writes = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            Duplex {
                close: gate.clone(),
                observe: duplex,
            },
            Rc::clone(&writes),
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
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

#[test]
fn trails_drain_reply() {
    let mut finish = Completion::new();
    let want = Pattern(12_000).bytes();
    let gate = Gate::new();
    let writes = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            script::App {
                payload: Some(want.clone()),
                close: gate.clone(),
                chunk: None,
                observe: script,
            },
            Rc::clone(&writes),
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
                        |stream| {
                            stream.write_all(b"GET\n")?;
                            Peer::try_read_all(stream)
                        },
                        |peer| {
                            reached(case.try_wait(&gate, 1), gate.hits(), 1);
                            let got = peer
                                .finish()
                                .expect("reply peer finish")
                                .expect("reply peer I/O");
                            let mut expected = want;
                            expected.extend_from_slice(BYE);
                            assert_eq!(got, expected);
                            assert_eq!(gate.hits(), 1);
                        },
                    )
                    .expect("spawn reply peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

#[test]
fn survives_peer_eof() {
    let mut finish = Completion::new();
    let gate = Gate::new();
    let writes = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            script::App {
                payload: None,
                close: gate.clone(),
                chunk: None,
                observe: script,
            },
            Rc::clone(&writes),
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
                        |stream| -> io::Result<Vec<u8>> {
                            stream.write_all(b"REQ")?;
                            stream.shutdown(Shutdown::Write)?;
                            Peer::try_read_all(stream)
                        },
                        |peer| {
                            reached(case.try_wait(&gate, 1), gate.hits(), 1);
                            let got = peer
                                .finish()
                                .expect("EOF peer finish")
                                .expect("EOF peer I/O");
                            assert_eq!(got, BYE);
                            assert_eq!(gate.hits(), 1);
                        },
                    )
                    .expect("spawn EOF peer");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            native,
        )
        .expect("listener run");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

#[test]
fn quiesces_buffered_send() {
    let mut finish = Completion::new();
    let chunk = Gate::new();
    let close = Gate::new();
    let writes = Rc::new(Trace::new());
    let prepared = Listener::new::<64>(Default::default())
        .prepare::<KeyTag<0>, _, Bundle<Tcp, Wire, Throughput>>((
            script::App {
                payload: None,
                close: close.clone(),
                chunk: Some(chunk.clone()),
                observe: script,
            },
            Rc::clone(&writes),
        ))
        .expect("listener layout");
    let mut reports =
        Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let (sender, receiver) = Channel::pair().expect("peer address channel");
    let ((), got) = run::Entry(move || {
        let port = match receiver.recv() {
            Record::Value(port) => u16::try_from(port).expect("listener port"),
            record => return Err(record),
        };
        Ok(script::Script(([127, 0, 0, 1], port).into()).try_read())
    })
    .with(|| {
        prepared
            .with_run(
                &mut finish,
                |case| {
                    let addr = case.addr();
                    assert_eq!(addr.ip(), std::net::IpAddr::from([127, 0, 0, 1]));
                    sender
                        .send(u32::from(addr.port()))
                        .expect("send listener port");
                    reached(case.try_wait(&chunk, 1), chunk.hits(), 1);
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                native,
            )
            .expect("listener run");
    })
    .expect("buffered peer start");
    assert!(
        reports.events().expect("reclaimed events").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    let got = got
        .expect("buffered peer join")
        .expect("peer address")
        .expect("buffered peer read");
    assert_eq!(got, BYE);
    assert_eq!(close.hits(), 1);
    let writes = writes.values().expect("wire writes");
    assert!(writes.is_empty(), "wire write failures: {writes:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
