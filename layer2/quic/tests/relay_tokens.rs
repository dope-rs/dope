pub mod common;

use common::{
    peer, recovery,
    runtime::relay,
    suite::{mux::dispatch, peers::tls, tokens},
};
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
    conn::life::{accept, options::Options, tls::servers},
    proto::{epoch, path::Peer},
    runtime::{
        endpoint::{self, owner::pool},
        mux::builder::Builder,
        receive, routing,
    },
    security::relay::{Key, Ticket},
    wire::packet::{long::initial, version},
};
use dope_test::harness::peer::{self as native, udp::Socket};
use dope_test::harness::scenario::TIMEOUT;
use std::{
    convert::Infallible,
    num::NonZero,
    pin::Pin,
    time::{Duration, Instant},
};

#[test]
fn new_token_binds_direct_and_relay_routes() {
    let key = Key::try_new().expect("ticket key");
    let ticket = key.issue(0, "10.0.0.1:5000".parse().expect("original peer"));
    let changed = key.issue(0, "10.0.0.1:5001".parse().expect("changed peer"));
    for original in [None, Some(ticket)] {
        let (identity, public) = peer::raw(0x39).expect("server identity");
        let options = Options::builder()
            .security(|security| security.retry([0x9a; 32]))
            .try_build()
            .expect("Retry policy");
        let authority =
            accept::Authority::<0>::new(identity.server().expect("server TLS"), options)
                .expect("server authority");
        let pool = servers::Pool::new(authority, NonZero::new(4).expect("handshake capacity"));
        let bind = routing::Single::new("127.0.0.1:0".parse().expect("native bind"));
        let plan = endpoint::Plan::<0, receive::Adaptive>::single(
            endpoint::config::Config {
                max_conns: 4,
                outgoing_capacity: 32,
                outgoing_bytes_capacity: 1200 * 32,
                packet_buffer_slots: 32,
                packet_buffer_bytes: 1200,
                ..common::retry::ENDPOINT
            },
            &bind,
        )
        .expect("relay endpoint plan");
        let settings = common::retry::Events::settings(64, &plan).expect("native resources");
        let socket = Socket::bind(([127, 0, 0, 1], 0).into()).expect("relay socket");
        let second = Socket::bind(([127, 0, 0, 1], 0).into()).expect("second relay socket");
        socket.nonblock(true);
        second.nonblock(true);
        let physical = socket.local_addr().expect("relay address");
        let mut client = Builder::client(tokens::Capture::default())
            .build()
            .expect("client router");
        let authority = tls::client(public).expect("client authority");
        let handle = Pin::new(&mut client)
            .observe(Instant::now(), |mut turn| {
                turn.protocol().connect(
                    tokens::SERVER,
                    &authority,
                    Options::default(),
                    [0x61; 8].to_vec(),
                )
            })
            .expect("client connection");
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
            .shutdown(source)
            .expect("shutdown source");
        let mut reports = Vec::new();
        let mut done = false;
        executor.run(|executor| {
            let result = executor.enter_with(
                |mut session| {
                    let (server, timeout) = {
                        let mut driver = session.driver();
                        let timeout = driver
                            .driver_ref()
                            .clock()
                            .now()
                            .try_add(Duration::from_secs(3))
                            .expect("deadline");
                        let server = pool::Binding::new(&pool)
                            .bind_single(bind, relay::Events::default(), plan, &mut driver)
                            .expect("single relay endpoint");
                        (server, timeout)
                    };
                    let address = server.local_addr();
                    let mut phase = 0;
                    let mut accepted = Vec::new();
                    let mut buffer = [0; 2048];
                    let mut finish = session::Completion::new();
                    session
                        .with_quiesce(
                            &mut finish,
                            relay::App {
                                server,
                                timeout,
                                poll: |server: &mut relay::Control<'_, '_, '_, '_>, clock| {
                                    if done {
                                        return control::Flow::Idle;
                                    }
                                    assert!(
                                        !timeout.is_due(clock.now()),
                                        "token test timed out in phase {phase}"
                                    );
                                    let now = clock.now().into();
                                    if phase == 0 {
                                        relay::receive(&socket, original, |packet| {
                                            Pin::new(&mut client).observe(now, |mut turn| {
                                                turn.protocol().recv(
                                                    tokens::SERVER,
                                                    packet,
                                                    epoch::WALL_CLOCK,
                                                )
                                            })
                                        })
                                        .expect("authenticated handshake response");
                                        for mut flight in
                                            dispatch::Capture::output(&mut client, now)
                                                .expect("client output")
                                        {
                                            for packet in flight.segments_mut() {
                                                relay::send(&socket, address, original, packet)
                                                    .expect("client relay send");
                                            }
                                        }
                                        let saved = Pin::new(&mut client)
                                            .observe(now, |mut turn| {
                                                turn.protocol().with_conn(handle, |mut scope| {
                                                    scope.security().try_saved()
                                                })
                                            })
                                            .expect("client connection")
                                            .expect("saved capability");
                                        if let Some(saved) = saved {
                                            assert_eq!(server.handler().created.len(), 1);
                                            let next = tokens::Capture::client(saved);
                                            next.as_ref()
                                                .map_err(common::suite::peers::Error::cause)
                                                .expect("reconnect setup");
                                            let mut next = next.ok().expect("reconnect client");
                                            let mut token = None;
                                            for mut flight in
                                                dispatch::Capture::output(&mut next, now)
                                                    .expect("reconnect Initial")
                                            {
                                                for packet in flight.segments_mut() {
                                                    if let Ok(prefix) =
                                                        initial::Prefix::decode(packet)
                                                    {
                                                        token = Some(prefix.token.to_vec());
                                                    }
                                                }
                                            }
                                            let token =
                                                token.expect("NEW_TOKEN reaches reconnect Initial");
                                            assert_eq!(token.len(), 24);
                                            accepted =
                                                peer::Initial::new(&[0x91; 8], &[0xa1; 8], 0)
                                                    .token(&token)
                                                    .seal(&[0x01])
                                                    .expect("token-bearing Initial");
                                            let other = Some(if original.is_some() {
                                                changed
                                            } else {
                                                ticket
                                            });
                                            relay::send(&socket, address, other, &accepted)
                                                .expect("changed ticket send");
                                            relay::send(&second, address, original, &accepted)
                                                .expect("changed physical send");
                                            if original.is_some() {
                                                relay::send(&socket, address, None, &accepted)
                                                    .expect("relay token on direct path");
                                            }
                                            relay::send(
                                                &socket,
                                                address,
                                                original,
                                                &relay::negotiation(),
                                            )
                                            .expect("barrier send");
                                            phase = 1;
                                        }
                                    } else if phase == 1 {
                                        loop {
                                            let len = match socket.recv(&mut buffer) {
                                                Ok(len) => len,
                                                Err(native::Error::Syscall(error))
                                                    if error.is_block() =>
                                                {
                                                    break;
                                                }
                                                Err(error) => panic!("relay receive: {error:?}"),
                                            };
                                            let wire =
                                                if matches!(buffer.first(), Some(0x14 | 0x16)) {
                                                    &buffer[Ticket::LEN..len]
                                                } else {
                                                    &buffer[..len]
                                                };
                                            if version::Negotiation::decode(wire).is_ok() {
                                                assert_eq!(
                                                    server.handler().created.len(),
                                                    1,
                                                    "changed routes require fresh validation"
                                                );
                                                relay::send(&socket, address, original, &accepted)
                                                    .expect("same full route");
                                                phase = 2;
                                                break;
                                            }
                                        }
                                    }
                                    if phase == 2 && server.handler().created.len() == 2 {
                                        let handle = server.handler().created[1];
                                        let (route, valid) = server
                                            .at(clock)
                                            .with_app(handle, |mut conn| {
                                                let state = conn.transport().view();
                                                (state.path().peer_route(), state.path().is_valid())
                                            })
                                            .expect("accepted token connection");
                                        let expected = original
                                            .map_or(Peer::direct(physical), |ticket| {
                                                Peer::relayed(physical, ticket)
                                            });
                                        assert_eq!(route, Some(expected));
                                        assert!(
                                            valid,
                                            "NEW_TOKEN validates only its complete route"
                                        );
                                        assert!(
                                            server.handler().received.is_empty(),
                                            "{:?}",
                                            server.handler().received
                                        );
                                        assert_eq!(server.handler().certificates, 0);
                                        assert_eq!(client.handler().certificates, 0);
                                        done = true;
                                        stop.take()
                                            .expect("one shutdown")
                                            .fire()
                                            .expect("shutdown");
                                        phase = 3;
                                    }
                                    if phase == 3 {
                                        control::Flow::Idle
                                    } else {
                                        control::Flow::More
                                    }
                                },
                            },
                            TIMEOUT,
                            |app| app.run_for(TIMEOUT).map(|_| ()),
                        )
                        .expect("session retirement")
                        .expect("native application");
                },
                Policy::new(
                    |event| reports.push(event),
                    |failure: Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause())
                    },
                ),
            );
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver retirement");
        });
        assert!(done);
        recovery::Teardown::new([Id::fixed::<0>()])
            .verify(reports)
            .expect("native retirement");
    }
}
