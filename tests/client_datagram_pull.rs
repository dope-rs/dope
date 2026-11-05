pub mod common;

use common::{
    peer, recovery,
    retry::{
        BIND, ENDPOINT, Events,
        traces::Immediate,
        writable::{Callback, Connected, Peer, Pull, PullControl},
    },
    suite::{mux::dispatch::Capture, peers::tls},
};
use dope::{
    core::driver::{
        life::{creation::Attempt, recovery::Policy},
        route::Id,
        schedule::clock,
    },
    runtime::{
        control,
        executor::{Executor, session},
        shutdown,
    },
};
use dope_quic::{
    client::{self, EndpointSpec, build},
    conn::{Epoch, life::options::Options},
    errors,
    proto::param,
    runtime::endpoint::{client::Plan, config::Config},
};
use dope_test::harness::scenario::TIMEOUT;
use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    num::NonZero,
    pin::Pin,
    time::Duration,
};

#[test]
fn retains_datagrams_until_pull_and_rotates_ready_sources() {
    let (signing, public) = peer::raw(0x75).expect("identity");
    let params = param::Set::builder()
        .idle_ms(30_000)
        .datagram(Some(1200))
        .initial_data(1024)
        .local_bidi(1024)
        .remote_bidi(1024)
        .bidi_count(1)
        .try_build()
        .expect("parameters");
    let mut peer = Peer::with_limit(
        signing.server().expect("server TLS"),
        Options::try_from(params.clone()).expect("peer options"),
        NonZero::new(3).expect("three sources"),
    )
    .expect("peer");
    let options = Options::try_from(params).expect("client options");
    let address = peer.addr().expect("peer address");
    let observed = peer.observations();
    let dial = Plan::<1>::new(
        BIND,
        Config {
            max_conns: 3,
            ..ENDPOINT
        },
    )
    .expect("client plan");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let mut create = Attempt::new();
    let executor = Executor::new(
        &mut create,
        Events::settings(16, dial.as_ref()).expect("driver claims"),
    );
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("driver");
    let executor = executor
        .ok()
        .expect("driver admission")
        .shutdown(source)
        .expect("shutdown source");
    let streams = Cell::new(0usize);
    let received = RefCell::new(Vec::<(client::AttemptId, Vec<u8>)>::new());
    let completed = Cell::new(false);
    let retry = Immediate::default();
    let mut reports = Vec::new();
    executor.run(|executor| {
        executor.enter_with(
            |mut session| {
                let (client, timeout, next) = {
                    let mut driver = session.driver();
                    let now = driver.driver_ref().clock().now();
                    let timeout = now.try_add(Duration::from_secs(2)).expect("deadline");
                    let authority = tls::client(public).expect("client TLS");
                    let endpoints = (0..3).map(|_| EndpointSpec {
                        addr: address,
                        authority: authority.try_clone().expect("authority reference"),
                    }).collect();
                    let client = build::Builder::owned(
                        endpoints,
                        options,
                        Connected::<_, true>::pull(|event| match event {
                            Ok(_) => {}
                            Err(Callback::Stream(_, data, end)) => {
                                if let Some(data) = data {
                                    assert_eq!(data, b"stream");
                                    streams.set(streams.get() + 1);
                                }
                                assert!(!end, "stream remains open during retained datagrams");
                            }
                            Err(cause) => panic!("unexpected callback: {cause:?}"),
                        }),
                        retry.clone(),
                        client::Config { event_budget: 8, retry_budget: 8 },
                    ).bind(dial, &mut driver).expect("client");
                    (client, timeout, now.into())
                };
                let mut stop = Some(stop);
                let mut phase = 0;
                let mut handles = Vec::new();
                let mut closed = None;
                let mut emitted = 0;
                let mut completion = session::Completion::new();
                session.with_quiesce(
                    &mut completion,
                    Pull {
                        client,
                        next,
                        poll: |client: &mut PullControl<'_, '_, '_, _>, clock: clock::Clock<'_, '_>, next: &mut clock::Deadline<'_>| {
                            if completed.get() {
                                return control::Flow::Idle;
                            }
                            let now = clock.now();
                            assert!(!timeout.is_due(now), "pull progress timed out in phase {phase}");
                            *next = now.try_add(Duration::from_millis(1)).expect("peer poll");
                            client.take();
                            peer.receive(now.into_raw()).expect("peer receive");
                            for flight in Capture::output(&mut peer.router, now.into_raw()).expect("peer output") {
                                if phase == 1 {
                                    emitted += flight.packet_count().get();
                                }
                                peer.send(flight).expect("native send");
                            }
                            match phase {
                                0 if client.protocol().len() == 3 => {
                                    handles = observed.borrow().established.clone();
                                    if handles.len() != 3 {
                                        return control::Flow::Idle;
                                    }
                                    assert!(client.at(clock).recv_datagram().expect("empty pull").is_none());
                                    for (source, handle) in handles.iter().copied().enumerate() {
                                        for sequence in 0..2 {
                                            Capture::send(&mut peer.router, handle, vec![source as u8, sequence], now.into_raw()).expect("datagram admission");
                                        }
                                        Pin::new(&mut peer.router).observe(now.into_raw(), |mut turn| {
                                            turn.protocol().with_conn(handle, |mut conn| {
                                                let mut streams = conn.streams();
                                                let mut send = streams.send();
                                                let stream = send.open_bidi().expect("stream admission");
                                                send.send(stream.send(), b"stream").expect("stream send");
                                            }).expect("peer connection");
                                        });
                                    }
                                    phase = 1;
                                }
                                1 if emitted >= 6 && streams.get() == 3 && handles.iter().all(|handle| {
                                    peer.router.inspect(*handle, |_, _, conn| conn.inspection().tx_queued()) == Some(0)
                                        && peer.router.conn(*handle).expect("peer retained").view().recovery().unacked(Epoch::Application) == 0
                                }) => {
                                    assert!(received.borrow().is_empty(), "no pop before consumer admission");
                                    // Stop periodic peer polling while the admitted consumer
                                    // takes one retained native head per coordination turn.
                                    *next = timeout;
                                    phase = 2;
                                }
                                2 => {
                                    *next = timeout;
                                    let (attempt, data) = client.at(clock).recv_datagram().expect("live pull").expect("retained native head");
                                    assert_eq!(data.len(), 2);
                                    assert_eq!(attempt.generation(), 1);
                                    let mut records = received.borrow_mut();
                                    if records.len() < 3 {
                                        assert_eq!(data[1], 0);
                                        assert!(records.iter().all(|(seen, bytes)| seen.slot() != attempt.slot() && bytes[0] != data[0]), "one head per source before its second packet");
                                    } else {
                                        assert_eq!(data[1], 1);
                                        assert_ne!(Some(attempt), closed);
                                        assert!(records.iter().any(|(seen, bytes)| *seen == attempt && bytes[0] == data[0] && bytes[1] == 0));
                                    }
                                    records.push((attempt, data));
                                    if records.len() == 3 {
                                        let attempt = records[0].0;
                                        client.seal();
                                        client.at(clock).with_attempt(attempt, |mut conn| conn.lifecycle().close(errors::Code::ZERO, Vec::new())).expect("close selected source");
                                        assert_eq!(client.at(clock).with_attempt(attempt, |_| ()), Err(errors::Access::Closed));
                                        closed = Some(attempt);
                                    } else if records.len() == 5 {
                                        assert!(client.at(clock).recv_datagram().expect("drained ready queue").is_none());
                                        client.at(clock).close_all();
                                        assert!(client.at(clock).recv_datagram().expect("closed ready queue").is_none());
                                        completed.set(true);
                                        stop.take().expect("shutdown owner").fire().expect("shutdown signal");
                                    }
                                }
                                _ => {}
                            }
                            control::Flow::Idle
                        },
                    },
                    TIMEOUT,
                    |app| app.run_for(TIMEOUT).map(|_| ()),
                ).expect("session completion").expect("application completion");
            },
            Policy::new(
                |event| reports.push(event),
                |failure: dope::core::driver::life::recovery::Recovery<'_, '_>| -> Infallible {
                    panic!("native recovery: {:?}", failure.cause())
                },
            ),
        ).as_ref().map_err(|cause| cause.cause()).expect("driver retirement");
    });
    assert!(completed.get());
    assert_eq!(received.borrow().len(), 5);
    assert!(retry.0.get().is_none());
    recovery::Teardown::new([Id::fixed::<1>()])
        .verify(reports)
        .expect("receive retirement");
}
