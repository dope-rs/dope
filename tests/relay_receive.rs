pub mod common;

use common::{peer, recovery, runtime::relay, suite::peers::packet};
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
    proto::{param, path::Peer},
    runtime::{
        endpoint::{self, owner::pool},
        receive, routing,
    },
    security::relay::{Key, Ticket},
    wire::{
        frame,
        packet::{cid, long::retry, version},
    },
};
use dope_test::harness::peer::{self as native, udp::Socket};
use dope_test::harness::scenario::TIMEOUT;
use std::{convert::Infallible, num::NonZero, time::Duration};

#[test]
fn retry_binds_full_route() {
    let (identity, _) = peer::raw(0x39).expect("server identity");
    let options = Options::builder()
        .security(|security| security.retry([0x9a; 32]))
        .try_build()
        .expect("Retry policy");
    let authority = accept::Authority::<0>::new(identity.server().expect("server TLS"), options)
        .expect("server authority");
    let pool = servers::Pool::new(authority, NonZero::new(4).expect("handshake capacity"));
    let bind = routing::Single::new("127.0.0.1:0".parse().expect("native bind"));
    let plan = endpoint::Plan::<0, receive::Adaptive>::single(
        endpoint::config::Config {
            max_conns: 4,
            outgoing_capacity: 8,
            outgoing_bytes_capacity: 1200 * 8,
            packet_buffer_slots: 8,
            packet_buffer_bytes: 1200,
            ..common::retry::ENDPOINT
        },
        &bind,
    )
    .expect("inner 1200 plus outer ticket");
    let settings = common::retry::Events::settings(32, &plan).expect("native resources");
    let first = Socket::bind(([127, 0, 0, 1], 0).into()).expect("first relay socket");
    let second = Socket::bind(([127, 0, 0, 1], 0).into()).expect("second relay socket");
    first.nonblock(true);
    second.nonblock(true);
    let physical = first.local_addr().expect("relay address");
    let key = Key::try_new().expect("ticket key");
    let ticket = key.issue(0, "10.0.0.1:5000".parse().expect("original peer"));
    let changed = key.issue(0, "10.0.0.1:5001".parse().expect("changed peer"));
    let dcid = [0x51; 8];
    let scid = [0x71; 8];
    let initial = peer::Initial::new(&dcid, &scid, 0)
        .seal(&[0x01])
        .expect("Initial");
    assert_eq!(initial.len(), 1200);
    let initial = relay::frame(Some(ticket), &initial).expect("request framing");
    assert_eq!(initial.len(), 1231);
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
                        .try_add(Duration::from_secs(2))
                        .expect("deadline");
                    let server = pool::Binding::new(&pool)
                        .bind_single(bind, relay::Events::default(), plan, &mut driver)
                        .expect("single relay endpoint");
                    (server, timeout)
                };
                let address = server.local_addr();
                assert_eq!(
                    first.send_to(&initial, address).expect("initial send"),
                    initial.len()
                );
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
                                    "relay test timed out in phase {phase}"
                                );
                                loop {
                                    let len = match first.recv_from(&mut buffer) {
                                        Ok((len, from)) => {
                                            assert_eq!(from, address);
                                            len
                                        }
                                        Err(native::Error::Syscall(error)) if error.is_block() => {
                                            break;
                                        }
                                        Err(error) => panic!("relay receive: {error:?}"),
                                    };
                                    assert_eq!(&buffer[..Ticket::LEN], ticket.as_bytes());
                                    let wire = &buffer[Ticket::LEN..len];
                                    match phase {
                                        0 => {
                                            let retry =
                                                retry::Ref::decode(wire).expect("framed Retry");
                                            let mut storage = Vec::new();
                                            let verified = retry
                                                .verify_into(
                                                    dope_quic::wire::packet::cid::Ref::new(&dcid)
                                                        .expect("original CID"),
                                                    dope_quic::wire::packet::cid::Ref::new(&scid)
                                                        .expect("client CID"),
                                                    &mut storage,
                                                )
                                                .expect("Retry verification")
                                                .expect("Retry integrity excludes ticket");
                                            accepted = peer::Initial::new(
                                                verified.scid().into_slice(),
                                                &scid,
                                                0,
                                            )
                                            .token(verified.token())
                                            .seal(&[0x01])
                                            .expect("retried Initial");
                                            let wrong_ticket =
                                                relay::frame(Some(changed), &accepted)
                                                    .expect("changed ticket framing");
                                            first
                                                .send_to(&wrong_ticket, address)
                                                .expect("changed ticket send");
                                            let wrong_physical =
                                                relay::frame(Some(ticket), &accepted)
                                                    .expect("changed physical framing");
                                            second
                                                .send_to(&wrong_physical, address)
                                                .expect("changed physical send");
                                            first
                                                .send_to(&accepted, address)
                                                .expect("direct replay of relay token");
                                            let barrier =
                                                relay::frame(Some(ticket), &relay::negotiation())
                                                    .expect("barrier framing");
                                            first.send_to(&barrier, address).expect("barrier send");
                                            phase = 1;
                                        }
                                        1 => {
                                            version::Negotiation::decode(wire)
                                                .expect("processing barrier");
                                            assert!(
                                                server.handler().created.is_empty(),
                                                "changed routes must not consume Retry proof"
                                            );
                                            let accepted = relay::frame(Some(ticket), &accepted)
                                                .expect("same-route framing");
                                            first
                                                .send_to(&accepted, address)
                                                .expect("same-route Retry proof");
                                            phase = 2;
                                        }
                                        _ => {}
                                    }
                                }
                                if phase == 2 && server.handler().created.len() == 1 {
                                    let handle = server.handler().created[0];
                                    let route = server
                                        .at(clock)
                                        .with_app(handle, |mut conn| {
                                            conn.transport().view().path().peer_route()
                                        })
                                        .expect("accepted connection");
                                    assert_eq!(route, Some(Peer::relayed(physical, ticket)));
                                    assert!(
                                        server.handler().received.is_empty(),
                                        "{:?}",
                                        server.handler().received
                                    );
                                    assert_eq!(server.handler().certificates, 0);
                                    done = true;
                                    stop.take().expect("one shutdown").fire().expect("shutdown");
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

#[test]
fn changed_ticket_validates_before_active_migration() {
    let params = param::Set::builder()
        .udp_max(1200)
        .datagram(Some(1200))
        .try_build()
        .expect("transport parameters");
    let options = Options::builder()
        .transport(|transport| transport.params(params.clone()))
        .try_build()
        .expect("connection options");
    let (identity, public) = peer::raw(0x5a).expect("server identity");
    let mut client = packet::Session::try_new(
        dope_tls::config::raw::Identity::client(public).expect("client TLS"),
        &params,
    )
    .expect("wire peer");
    let authority = accept::Authority::<0>::new(identity.server().expect("server TLS"), options)
        .expect("server authority");
    let pool = servers::Pool::new(authority, NonZero::<usize>::MIN);
    let bind = routing::Single::new("127.0.0.1:0".parse().expect("native bind"));
    let plan = endpoint::Plan::<0, receive::Adaptive>::single(
        endpoint::config::Config {
            outgoing_capacity: 1,
            outgoing_bytes_capacity: 1200,
            packet_buffer_bytes: 1500,
            ..common::retry::ENDPOINT
        },
        &bind,
    )
    .expect("relay endpoint plan");
    let settings = common::retry::Events::settings(32, &plan).expect("native resources");
    let socket = Socket::bind(([127, 0, 0, 1], 0).into()).expect("relay socket");
    socket.nonblock(true);
    let physical = socket.local_addr().expect("relay address");
    let key = Key::try_new().expect("ticket key");
    let original = key.issue(0, "10.0.0.1:5000".parse().expect("original peer"));
    let changed = key.issue(0, "10.0.0.1:5001".parse().expect("changed peer"));
    let candidate_cid = [0x65; 8];
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
                        .try_add(Duration::from_secs(2))
                        .expect("deadline");
                    let server = pool::Binding::new(&pool)
                        .bind_single(bind, relay::Events::default(), plan, &mut driver)
                        .expect("native relay endpoint");
                    (server, timeout)
                };
                let remote = server.local_addr();
                relay::send(
                    &socket,
                    remote,
                    Some(original),
                    &client.initial().expect("Initial"),
                )
                .expect("initial relay input");
                let mut phase = 0;
                let mut challenges = [None; 2];
                let mut delivered = false;
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
                                    "ticket migration timed out in phase {phase}"
                                );
                                relay::receive(&socket, None, |wire| {
                                    assert!(
                                        wire.len() > Ticket::LEN
                                            && wire.len() <= 1200 + Ticket::LEN
                                    );
                                    let (prefix, inner) = wire.split_at_mut(Ticket::LEN);
                                    assert!(
                                        prefix == original.as_bytes()
                                            || prefix == changed.as_bytes()
                                    );
                                    let previous = prefix == original.as_bytes();
                                    let Some(mut packet) = client.receive(inner)? else {
                                        assert!(previous, "handshake stays on the original route");
                                        return Ok(());
                                    };
                                    let (_, body) = client.open(&mut packet)?;
                                    assert_eq!(
                                        &packet[1..1 + packet::CID.len()],
                                        if previous {
                                            &packet::CID
                                        } else {
                                            &candidate_cid
                                        },
                                        "each full route retains its own destination CID"
                                    );
                                    for frame in frame::Frame::decode_all(&body)
                                        .map_err(packet::Error::Frame)?
                                    {
                                        match frame.view() {
                                            frame::View::PathChallenge { data } => {
                                                assert!(phase >= 2);
                                                challenges[usize::from(!previous)] = Some(*data);
                                            }
                                            frame::View::Datagram { data, .. } => {
                                                assert_eq!(phase, 4);
                                                assert_eq!(prefix, changed.as_bytes());
                                                assert_eq!(*data, b"migrated");
                                                delivered = true;
                                            }
                                            _ => {}
                                        }
                                    }
                                    Ok::<(), packet::Error>(())
                                })
                                .expect("native framed output");
                                if phase == 0 {
                                    match client.finish(None) {
                                        Ok(packet) => {
                                            relay::send(&socket, remote, Some(original), &packet)
                                                .expect("handshake finish");
                                            phase = 1;
                                        }
                                        Err(packet::Error::NotDone) => return control::Flow::More,
                                        Err(error) => panic!("handshake finish: {error:?}"),
                                    }
                                }
                                let Some(&handle) = server.handler().established.first() else {
                                    return control::Flow::More;
                                };
                                let active = server
                                    .at(clock)
                                    .with_app(handle, |mut conn| {
                                        conn.transport().view().path().peer_route()
                                    })
                                    .expect("live connection route");
                                if phase == 1 {
                                    assert_eq!(active, Some(Peer::relayed(physical, original)));
                                    let notice = packet::bytes::notice_with_id(
                                        1,
                                        0,
                                        cid::Ref::new(&candidate_cid).expect("candidate CID"),
                                    )
                                    .expect("spare CID announcement");
                                    let packet = client.packet(&notice).expect("spare CID packet");
                                    relay::send(&socket, remote, Some(original), &packet)
                                        .expect("spare CID on the original route");
                                    let mut frames = [0; 500];
                                    frames[0] = frame::TYPE_PING;
                                    let packet =
                                        client.packet(&frames).expect("non-probing 1RTT packet");
                                    assert_eq!(packet[0] & 0x80, 0);
                                    relay::send(&socket, remote, Some(changed), &packet)
                                        .expect("changed ticket input");
                                    phase = 2;
                                } else if phase == 2 {
                                    assert_eq!(
                                        active,
                                        Some(Peer::relayed(physical, original)),
                                        "unvalidated ticket must not become active"
                                    );
                                    if let [Some(previous), Some(candidate)] = challenges {
                                        assert_ne!(previous, candidate, "independent path nonces");
                                        for (ticket, data) in
                                            [(original, previous), (changed, candidate)]
                                        {
                                            let mut frames = [0; 500];
                                            frames[0] = frame::TYPE_PATH_RESPONSE;
                                            frames[1..9].copy_from_slice(&data);
                                            let packet =
                                                client.packet(&frames).expect("1RTT path response");
                                            relay::send(&socket, remote, Some(ticket), &packet)
                                                .expect("response on its challenged route");
                                        }
                                        phase = 3;
                                    }
                                } else if phase == 3
                                    && active == Some(Peer::relayed(physical, changed))
                                {
                                    server
                                        .at(clock)
                                        .with_conn(handle, |mut conn| {
                                            conn.datagrams().try_send(b"migrated".to_vec())
                                        })
                                        .expect("migrated connection")
                                        .expect("application output");
                                    phase = 4;
                                }
                                if delivered && !done {
                                    assert_eq!(active, Some(Peer::relayed(physical, changed)));
                                    assert_eq!(server.handler().created.len(), 1);
                                    assert!(
                                        server.handler().received.is_empty(),
                                        "{:?}",
                                        server.handler().received
                                    );
                                    assert_eq!(server.handler().certificates, 0);
                                    assert_eq!(server.handler().socket_failures, 0);
                                    done = true;
                                    stop.take().expect("one shutdown").fire().expect("shutdown");
                                }
                                if done {
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
