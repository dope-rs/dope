pub mod common;

use std::{cell::Cell, convert::Infallible, io, net::UdpSocket, sync::Arc, time::Duration};

use common::{
    peer, recovery,
    retry::{
        BIND, ENDPOINT, Events,
        traces::Immediate,
        writable::{Callback, Connected, Pull, PullControl},
    },
    suite::peers::{packet, tls},
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
    proto::param,
    runtime::endpoint::{client::Plan, config::Config},
    wire::{
        frame,
        packet::{cid, long::initial},
    },
};
use dope_test::harness::scenario::TIMEOUT;

#[test]
fn connects_before_stream_delivery() {
    let (signing, public) = peer::raw(0x7d).expect("raw identity");
    let server = Arc::new(signing.server().expect("server TLS"));
    let params = param::Set::builder()
        .idle_ms(30_000)
        .initial_data(1024)
        .local_bidi(1024)
        .remote_bidi(1024)
        .bidi_count(1)
        .try_build()
        .expect("stream parameters");
    let peers: [UdpSocket; 2] = std::array::from_fn(|_| {
        let peer = UdpSocket::bind(BIND).expect("peer socket");
        peer.set_nonblocking(true).expect("nonblocking peer");
        peer
    });
    let mut sessions: [Option<packet::params::Server>; 2] = [None, None];
    let plan = Plan::<1>::new(
        BIND,
        Config {
            max_conns: 2,
            ..ENDPOINT
        },
    )
    .expect("two client connections");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let mut create = Attempt::new();
    let executor = Executor::new(
        &mut create,
        Events::settings(16, plan.as_ref()).expect("driver claims"),
    );
    executor
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("driver");
    let executor = executor
        .ok()
        .expect("driver admission")
        .shutdown(source)
        .expect("shutdown source");
    let connected = Cell::new(0u8);
    let ready_in_turn = Cell::new(0usize);
    let submitted = Cell::new(0u8);
    let pending = Cell::new(0u8);
    let received = Cell::new(0u8);
    let completed = Cell::new(false);
    let mut reports = Vec::new();
    executor.run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let (client, timeout, next) = {
                        let mut driver = session.driver();
                        let now = driver.driver_ref().clock().now();
                        let timeout = now
                            .try_add(Duration::from_secs(2))
                            .expect("test deadline");
                        let authority = tls::client(public).expect("client TLS");
                        let endpoints = peers
                            .iter()
                            .map(|peer| EndpointSpec {
                                addr: peer.local_addr().expect("peer address"),
                                authority: authority.try_clone().expect("authority reference"),
                            })
                            .collect();
                        let client = build::Builder::owned(
                            endpoints,
                            Options::try_from(params.clone()).expect("client options"),
                            Connected::<_, true>::pull(|event| match event {
                                Ok(slot) => {
                                    assert_eq!(
                                        ready_in_turn.replace(ready_in_turn.get() + 1),
                                        0,
                                        "one ready callback per driver turn"
                                    );
                                    let bit = 1 << slot.index();
                                    assert!(submitted.get() & bit != 0);
                                    assert_eq!(received.get() & bit, 0, "coalesced input was pending");
                                    pending.set(pending.get() | bit);
                                    connected.set(connected.get() | bit);
                                }
                                Err(Callback::Stream(_, Some(data), _)) => {
                                    let [slot] = data.as_slice() else {
                                        panic!("one source byte");
                                    };
                                    assert!(
                                        connected.get() & (1 << *slot) != 0,
                                        "stream before connect: source={slot}, connected={}",
                                        connected.get()
                                    );
                                    let bit = 1 << *slot;
                                    assert_eq!(received.get() & bit, 0);
                                    received.set(received.get() | bit);
                                }
                                Err(Callback::Stream(_, None, true) | Callback::Close) => {}
                                event => panic!("unexpected callback: {event:?}"),
                            }),
                            Immediate::default(),
                            client::Config {
                                event_budget: 1,
                                retry_budget: 2,
                            },
                        )
                        .expect("client builder")
                        .bind(plan, &mut driver)
                        .expect("bound client");
                        (client, timeout, now.into())
                    };
                    let mut stop = Some(stop);
                    let mut completion = session::Completion::new();
                    session
                        .with_quiesce(
                            &mut completion,
                            Pull {
                                client,
                                next,
                                poll: |client: &mut PullControl<'_, '_, '_, _>,
                                       clock: clock::Clock<'_, '_>,
                                       next: &mut clock::Deadline<'_>| {
                                    if completed.get() {
                                        return control::Flow::Idle;
                                    }
                                    ready_in_turn.set(0);
                                    let now = clock.now();
                                    assert!(!timeout.is_due(now), "stream delivery timed out");
                                    *next = now
                                        .try_add(Duration::from_millis(1))
                                        .expect("peer poll");
                                    for (index, peer) in peers.iter().enumerate() {
                                        let bit = 1 << index;
                                        if submitted.get() & bit != 0 {
                                            continue;
                                        }
                                        let mut input = [0; 4096];
                                        loop {
                                            let (length, from) = match peer.recv_from(&mut input) {
                                                Ok(received) => received,
                                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                                                Err(error) => panic!("peer receive: {error:?}"),
                                            };
                                            if sessions[index].is_none() {
                                                let prefix = initial::Prefix::decode(&input[..length])
                                                    .expect("client Initial");
                                                let original = prefix.dcid.into_owned();
                                                let local = cid::Id::try_from([0x59 + index as u8; 8])
                                                    .expect("server CID");
                                                let mut encoded = Vec::new();
                                                params.bind_server(&local, &original, None, None)
                                                    .encode(&mut encoded).expect("server parameters");
                                                sessions[index] = Some(packet::params::Server::try_initial(
                                                    Arc::clone(&server), local, original, encoded,
                                                ).expect("server handshake"));
                                            }
                                            let session = sessions[index].as_mut().expect("peer session");
                                            assert!(session.receive(&input[..length]).expect("client handshake").is_none());
                                            let stream = [frame::TYPE_STREAM_BASE | 3, 1, 1, index as u8];
                                            let application = match session.packet(&stream) {
                                                Ok(packet) => packet,
                                                Err(packet::Error::Epoch(Epoch::Application)) => continue,
                                                Err(error) => panic!("server application packet: {error:?}"),
                                            };
                                            let flights = session.send().expect("server flight");
                                            assert_eq!(flights.len(), 2, "Initial and Handshake precede application data");
                                            let mut output = flights.into_iter().flatten().collect::<Vec<_>>();
                                            output.extend(application);
                                            assert_eq!(connected.get() & bit, 0);
                                            assert_eq!(peer.send_to(&output, from).expect("coalesced native send"), output.len());
                                            submitted.set(submitted.get() | bit);
                                            break;
                                        }
                                    }
                                    if received.get() == 3 {
                                        assert_eq!(pending.get(), 3);
                                        client.seal();
                                        completed.set(true);
                                        stop.take()
                                            .expect("shutdown owner")
                                            .fire()
                                            .expect("shutdown signal");
                                    }
                                    control::Flow::Idle
                                },
                            },
                            TIMEOUT,
                            |app| app.run_for(TIMEOUT).map(|_| ()),
                        )
                        .expect("session completion")
                        .expect("application completion");
                },
                Policy::new(
                    |event| reports.push(event),
                    |failure: dope::core::driver::life::recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            )
            .as_ref()
            .map_err(|cause| cause.cause())
            .expect("driver retirement");
    });
    assert!(completed.get());
    recovery::Teardown::new([Id::fixed::<1>()])
        .verify(reports)
        .expect("exact receive retirement");
}
