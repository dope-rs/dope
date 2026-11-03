pub mod common;

use common::{peer, peer::Receiver as _, recovery, runtime::relay, suite::peers::tls};
use dope::{
    core::driver::{
        life::{
            creation::Attempt,
            recovery::{Policy, Recovery},
        },
        route::Id,
    },
    runtime::{
        control,
        executor::{Executor, session},
        shutdown,
    },
};
use dope_quic::{
    conn::{
        self,
        life::{accept, options::Options, tls::servers},
        output::transmit::Admission,
    },
    errors::SendFailure,
    proto::{
        datagram::{Arena, Error},
        param,
        path::Peer,
    },
    runtime::{
        endpoint::{self, owner::pool},
        receive, routing,
    },
    security::relay::{Key, Ticket},
    wire::packet::cid,
};
use dope_test::harness::peer::udp::Socket;
use dope_test::harness::scenario::TIMEOUT;
use o3::buffer::{Residency as _, pool::borrowed, storage};
use std::{
    convert::Infallible,
    num::NonZero,
    time::{Duration, Instant},
};

#[test]
fn frames_each_segment_and_keeps_pooled_payload() {
    const PAYLOAD: usize = 1169;
    let params = param::Set::builder()
        .idle_ms(30_000)
        .udp_max(1200)
        .initial_data(1 << 20)
        .stream_uni(1 << 20)
        .uni_count(4)
        .datagram(Some(1200))
        .try_build()
        .expect("transport parameters");
    let options = || {
        Options::builder()
            .transport(|transport| transport.params(params.clone()).max_pmtu(1200))
            .try_build()
            .expect("connection options")
    };
    let (identity, public) = peer::raw(0x58).expect("server identity");
    let authority = accept::Authority::<0>::new(identity.server().expect("server TLS"), options())
        .expect("server authority");
    let pool = servers::Pool::new(authority, NonZero::<usize>::MIN);
    let bind = routing::Single::new("127.0.0.1:0".parse().expect("server bind"));
    let plan = endpoint::Plan::<0, receive::Adaptive>::single(
        endpoint::config::Config {
            packet_buffer_bytes: 1200,
            ..common::retry::ENDPOINT
        },
        &bind,
    )
    .expect("relay endpoint plan");
    let settings = common::retry::Events::settings(32, &plan).expect("driver resources");
    let socket = Socket::bind(([127, 0, 0, 1], 0).into()).expect("relay socket");
    socket.nonblock(true);
    let physical = socket.local_addr().expect("relay address");
    let ticket = Key::try_new()
        .expect("ticket key")
        .issue(0, "10.0.0.1:5010".parse().expect("original peer"));
    let client = tls::client(public).expect("client authority");
    let mut client = client
        .connect::<0, Vec<u8>, storage::Shared>(
            cid::Id::try_from([0x63; 8]).expect("local connection ID"),
            cid::Id::try_from([0x64; 8]).expect("remote connection ID"),
            options(),
            routing::Plain,
        )
        .expect("client connection");
    let mut work = conn::Workspace::try_new().expect("receive workspace");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let mut stop = Some(stop);
    let mut creation = Attempt::new();
    let executor = Executor::new(&mut creation, settings);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("driver");
    let executor = executor
        .ok()
        .expect("admitted driver")
        .with_storage((
            Arena::try_new(1, 1200).expect("inner slot"),
            Arena::try_new(1, 1231).expect("outer slot"),
        ))
        .shutdown(source)
        .expect("shutdown source");
    let mut reports = Vec::new();
    let mut done = false;
    executor.run(|executor| {
        let result = executor.enter_with(|mut session| {
            let (small, arena) = session.storage();
            let (server, timeout) = {
                let mut driver = session.driver();
                let timeout = driver.driver_ref().clock().now().try_add(Duration::from_secs(2)).expect("deadline");
                let server = pool::Binding::new(&pool).bind_single(bind, relay::Events::default(), plan, &mut driver)
                    .expect("native relay endpoint");
                (server, timeout)
            };
            let remote = server.local_addr();
            let mut stream = None;
            let mut body = Vec::new();
            let mut stream_complete = false;
            let mut rejected = false;
            let mut pending = None;
            let mut payload_pointer = 0;
            let mut sent = false;
            let mut delivered = false;
            let mut finish = session::Completion::new();
            session.with_quiesce(&mut finish, relay::App {
                server,
                timeout,
                poll: |server: &mut relay::Control<'_, '_, '_, '_>, clock| {
                    if done {
                        return control::Flow::Idle;
                    }
                    assert!(!timeout.is_due(clock.now()), "relay output deadline");
                    let now: Instant = clock.now().into();
                    relay::receive(&socket, Some(ticket), |inner| {
                        assert!(inner.len() <= 1200, "ticket must not consume the QUIC cap");
                        client.receive(&mut work, inner, now)
                    }).expect("framed native output");
                    let (packets, result) = client.with_scope(|mut scope| scope.output().send(now));
                    result.expect("client output");
                    for packet in packets {
                        relay::send(&socket, remote, Some(ticket), &packet).expect("relay input");
                    }
                    let Some(&handle) = server.handler().established.first() else {
                        return control::Flow::More;
                    };
                    if stream.is_none() {
                        server.enable_gso().expect("native segmentation");
                        stream = Some(server.at(clock).with_conn(handle, |mut conn| {
                            let mut streams = conn.streams();
                            let mut send = streams.send();
                            let stream = send.open_uni().expect("stream credit");
                            send.send(stream, &[0x71; 8192]).expect("stream payload");
                            send.finish(stream).expect("stream FIN");
                            stream.id()
                        }).expect("server connection"));
                    }
                    if !stream_complete {
                        stream_complete = client.with_scope(|mut scope| {
                            let mut streams = scope.streams();
                            let Ok(receive) = streams.view().resolve_recv(stream.expect("opened stream")) else {
                                return false;
                            };
                            streams.receive().recv(receive, &mut body).expect("stream receive");
                            let receive = streams.view().resolve_recv(stream.expect("opened stream")).expect("retained stream");
                            streams.view().is_recv_eof(receive)
                        });
                        if !stream_complete {
                            return control::Flow::More;
                        }
                        assert_eq!(body, [0x71; 8192]);
                    }
                    if !rejected {
                        let mut packet = small.try_acquire(PAYLOAD).expect("small owner");
                        packet.payload_mut().fill(0xa5);
                        let pointer = packet.payload().as_ptr();
                        match server.at(clock).try_pooled(handle, packet) {
                            Err(SendFailure::Full(_)) => return control::Flow::More,
                            Err(SendFailure::Admission(packet, Admission::Datagram(Error::Storage(borrowed::Error::Prefix { requested, available })))) => {
                                assert!(requested > available as usize);
                                assert_eq!(packet.payload().as_ptr(), pointer);
                                assert_eq!(packet.payload(), [0xa5; PAYLOAD]);
                                assert_eq!(small.available(), 0);
                                drop(packet);
                                assert_eq!(small.available(), 1);
                                rejected = true;
                            }
                            _ => panic!("missing exact prefix rejection"),
                        }
                    }
                    if !sent {
                        let packet = pending.take().unwrap_or_else(|| {
                            let mut packet = arena.try_acquire(PAYLOAD).expect("outer owner");
                            packet.payload_mut().fill(0xa5);
                            assert_eq!(packet.resident(), 1231);
                            payload_pointer = packet.payload().as_ptr() as usize;
                            packet
                        });
                        match server.at(clock).try_pooled(handle, packet) {
                            Ok(()) => sent = true,
                            Err(SendFailure::Full(packet)) => {
                                assert_eq!(packet.payload().as_ptr() as usize, payload_pointer);
                                pending = Some(packet);
                            }
                            _ => panic!("outer slot admission"),
                        }
                    }
                    if let Some(data) = client.with_scope(|mut scope| scope.datagrams().recv()) {
                        assert_eq!(data, [0xa5; PAYLOAD]);
                        delivered = true;
                    }
                    if sent && delivered && arena.available() == 1 && !done {
                        let events = server.handler();
                        assert!(events.accepted.iter().any(|(_, shape)| shape.is_segmented()), "stream must use GSO");
                        for (_, shape) in &events.accepted {
                            if let Some(size) = shape.segment_size() {
                                assert!(usize::from(size.get()) <= 1200 + Ticket::LEN);
                            }
                        }
                        assert!(events.settled.iter().any(|&(start, len)| len >= PAYLOAD + 16 + Ticket::LEN && start + len - 16 - PAYLOAD == payload_pointer), "kernel completion must retain the original pooled payload location");
                        assert!(events.received.is_empty(), "{:?}", events.received);
                        assert_eq!(events.certificates, 0);
                        assert_eq!(events.socket_failures, 0);
                        assert_eq!(server.at(clock).with_app(handle, |mut conn| conn.transport().view().path().peer_route()).expect("peer route"), Some(Peer::relayed(physical, ticket)));
                        done = true;
                        stop.take().expect("one shutdown").fire().expect("shutdown");
                    }
                    if done { control::Flow::Idle } else { control::Flow::More }
                },
            }, TIMEOUT, |app| app.run_for(TIMEOUT).map(|_| ()))
                .expect("session retirement").expect("native application");
            assert_eq!(arena.available(), 1);
        }, Policy::new(|event| reports.push(event), |failure: Recovery<'_, '_>| -> Infallible {
            panic!("native recovery: {:?}", failure.cause())
        }));
        result.as_ref().map_err(|error| error.cause()).expect("driver retirement");
    });
    assert!(done);
    recovery::Teardown::new([Id::fixed::<0>()])
        .verify(reports)
        .expect("native retirement");
}
