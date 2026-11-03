use common::suite::mux::dispatch;
pub mod common;

use common::suite::mux::paths::{self, CLIENT, Pair, Pending, SERVER};
use common::suite::peers::packet::bytes;
use dope_quic::conn;
use dope_quic::errors::SendFailure;
use dope_quic::proto::epoch;
use dope_quic::wire::frame;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

#[test]
fn foreign_does_not_credit() {
    let mut pending = Pending::try_new(0x6d, 0xa7).expect("paths fixture must open");
    let foreign = SocketAddr::from(([198, 51, 100, 9], 41_000));
    let at = pending.start;
    let mut initial =
        dispatch::Capture::output(&mut pending.client, at).expect("path fixture operation");
    assert!(!initial.is_empty());
    let replay = initial
        .iter()
        .map(|packet| packet.payload().to_vec())
        .collect::<Vec<_>>();
    Pair::deliver(&mut initial, CLIENT, &mut pending.server, at).expect("path fixture operation");
    let ids = &pending.server.handler().created;
    let handle = *ids
        .first()
        .expect("valid Initial must create one server handle");
    let credited = Pair::credit(&mut pending.server, handle, at).expect("path fixture operation");
    assert_ne!(credited, 0);
    for mut packet in replay {
        Pair::receive(&mut pending.server, foreign, &mut packet, at)
            .expect("path fixture operation");
    }
    assert_eq!(
        Pair::credit(&mut pending.server, handle, at).expect("path fixture operation"),
        credited
    );
    for router in [&pending.client, &pending.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}

#[test]
fn rejects_unknown_server() {
    let pending = Pending::try_new(0x39, 0x81).expect("paths fixture must open");
    let mut pair = pending.establish().expect("path fixture operation");
    let unknown = SocketAddr::from(([10, 0, 0, 3], 443));
    let at = pair.start + Duration::from_secs(1);
    pair.send_server(b"wrong-server-path".to_vec(), at)
        .expect("path fixture operation");
    let mut packets =
        dispatch::Capture::output(&mut pair.server, at).expect("path fixture operation");
    assert!(!packets.is_empty());
    for packet in &mut packets {
        for segment in packet.segments_mut() {
            let error = Pin::new(&mut pair.client).observe(at, |mut turn| {
                turn.protocol().recv(unknown, segment, epoch::WALL_CLOCK)
            });
            assert!(matches!(error, Err(dope_quic::errors::Receive::Packet(
                conn::Error::Path(conn::wire::path::Error::Policy { active, source })
            )) if active.addr() == SERVER && source.addr() == unknown));
        }
    }
    Pair::discard(&mut pair.client, at).expect("path fixture operation");
    assert!(pair.client.handler().datagrams.is_empty());
    assert_eq!(
        Pair::peer(&mut pair.client, pair.client_id, at).expect("path fixture operation"),
        Some(SERVER)
    );
    for router in [&pair.client, &pair.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}

#[test]
fn validates_before_rebind() {
    let pending = Pending::try_new(0x39, 0x81).expect("paths fixture must open");
    let mut pair = pending.establish().expect("path fixture operation");
    let rebound = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = pair.start + Duration::from_millis(20);
    let received = pair
        .observe(rebound, b"from-new-port".to_vec(), pair.start, at)
        .expect("path fixture operation");
    assert_eq!(
        Pair::peer(&mut pair.server, pair.server_id, at).expect("path fixture operation"),
        Some(CLIENT)
    );
    pair.send_server(b"before-validation".to_vec(), at)
        .expect("path fixture operation");
    let stream = Pair::access(&mut pair.server, pair.server_id, at, |mut conn| {
        let mut streams = conn.streams();
        let mut send = streams.send();
        let id = send
            .open_uni()
            .expect("the negotiated unidirectional stream credit must remain available");
        send.send(id, b"survives-paths")
            .expect("the fixture stream payload must fit its send credit");
        send.finish(id)
            .expect("the fixture send stream must remain open for FIN");
        id.id()
    })
    .expect("path fixture operation");
    let mut flight =
        dispatch::Capture::output(&mut pair.server, at).expect("path fixture operation");
    assert_eq!(
        pair.server.handler().datagrams,
        vec![b"from-new-port".to_vec()]
    );
    assert!(Pair::path(&flight, rebound).next().is_some());
    assert!(Pair::path(&flight, rebound).all(|packet| packet.payload().len() >= 1_200));
    let sent = Pair::path(&flight, rebound)
        .map(|packet| packet.payload().len())
        .sum::<usize>();
    assert!(sent <= received.saturating_mul(3));
    assert!(Pair::path(&flight, CLIENT).next().is_some());
    let active = Pair::path(&flight, CLIENT)
        .find_map(Pair::short_cid)
        .expect("the active path must emit a short-header packet");
    let probe = Pair::path(&flight, rebound)
        .find_map(Pair::short_cid)
        .expect("the candidate path must emit a short-header packet");
    assert_ne!(active, probe);
    let reply_at = pair
        .answer(&mut flight, rebound, at)
        .expect("path fixture operation");
    let route =
        Pair::peer(&mut pair.server, pair.server_id, reply_at).expect("path fixture operation");
    assert_eq!(route, Some(rebound));
    pair.send_server(b"after-validation".to_vec(), reply_at)
        .expect("path fixture operation");
    let after = reply_at + Duration::from_millis(1);
    let mut output =
        dispatch::Capture::output(&mut pair.server, after).expect("path fixture operation");
    assert!(Pair::path(&output, rebound).any(|packet| !packet.payload().is_empty()));
    Pair::relay(&mut output, rebound, SERVER, &mut pair.client, after)
        .expect("path fixture operation");
    Pair::discard(&mut pair.client, after).expect("path fixture operation");
    let streams = vec![(stream.get(), b"survives-paths".to_vec())];
    assert_eq!(pair.client.handler().streams, streams);
    let foreign = pair.client_id;
    let result = Pin::new(&mut pair.server).observe(after, |mut turn| {
        turn.protocol().try_datagram(foreign, Vec::new())
    });
    assert_eq!(result, Err(SendFailure::Closed(Vec::new())));
    for router in [&pair.client, &pair.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}

#[test]
fn new_candidate_supersedes() {
    let pending = Pending::try_new(0x39, 0x81).expect("paths fixture must open");
    let mut pair = pending.establish().expect("path fixture operation");
    let first = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let second = SocketAddr::from(([10, 0, 0, 1], 50_002));
    let first_at = pair.start + Duration::from_millis(20);
    pair.observe(first, vec![0x11; 500], first_at, first_at)
        .expect("path fixture operation");
    let flight =
        dispatch::Capture::output(&mut pair.server, first_at).expect("path fixture operation");
    assert!(Pair::path(&flight, first).next().is_some());
    let next = first_at + Duration::from_millis(20);
    pair.observe(second, vec![0x22; 500], next, next)
        .expect("path fixture operation");
    let mut flight =
        dispatch::Capture::output(&mut pair.server, next).expect("path fixture operation");
    assert!(Pair::path(&flight, second).next().is_some());
    assert!(Pair::path(&flight, first).next().is_none());
    let at = pair
        .answer(&mut flight, second, next)
        .expect("path fixture operation");
    pair.send_server(b"replacement-won".to_vec(), at)
        .expect("path fixture operation");
    let output = dispatch::Capture::output(&mut pair.server, at + Duration::from_millis(1))
        .expect("path fixture operation");
    assert!(Pair::path(&output, second).next().is_some());
    for router in [&pair.client, &pair.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}

#[test]
fn migration_retires_oversized_datagram_head() {
    let mut pair = Pending::try_new(0x39, 0x81)
        .expect("path peers")
        .establish()
        .expect("handshake");
    let mut at = pair.start;
    for step in 1..=16 {
        at = pair.start + Duration::from_millis(step * 100);
        let mut flight = dispatch::Capture::output(&mut pair.client, at).expect("client output");
        Pair::deliver(&mut flight, CLIENT, &mut pair.server, at).expect("client delivery");
        let mut flight = dispatch::Capture::output(&mut pair.server, at).expect("server output");
        Pair::deliver(&mut flight, SERVER, &mut pair.client, at).expect("server delivery");
    }
    let mtu = Pair::access(&mut pair.server, pair.server_id, at, |conn| {
        conn.view().path().mtu()
    })
    .expect("old path MTU");
    assert!(mtu > 1_400);

    let rebound = SocketAddr::from(([10, 0, 0, 1], 50_001));
    at += Duration::from_millis(100);
    pair.observe(rebound, vec![0x11; 500], at, at)
        .expect("migration intent");
    let mut flight = dispatch::Capture::output(&mut pair.server, at).expect("path challenge");
    pair.send_server(vec![0x77; 1_300], at)
        .expect("old MTU payload");
    pair.send_server(vec![0x42; 16], at).expect("small tail");
    at = pair
        .answer(&mut flight, rebound, at)
        .expect("path validation");
    assert_eq!(
        Pair::access(&mut pair.server, pair.server_id, at, |conn| {
            conn.view().path().mtu()
        })
        .expect("migrated MTU"),
        1_200
    );

    at += Duration::from_millis(100);
    let mut flight = dispatch::Capture::output(&mut pair.server, at).expect("migrated output");
    Pair::relay(&mut flight, rebound, SERVER, &mut pair.client, at).expect("small tail delivery");
    Pair::discard(&mut pair.client, at).expect("client notification");
    assert_eq!(pair.client.handler().datagrams, vec![vec![0x42; 16]]);
}

#[test]
fn expired_path_restarts() {
    let pending = Pending::try_new(0x39, 0x81).expect("paths fixture must open");
    let mut pair = pending.establish().expect("path fixture operation");
    let peer = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let first = pair.start + Duration::from_millis(20);
    pair.observe(peer, vec![0x33; 500], first, first)
        .expect("path fixture operation");
    let output =
        dispatch::Capture::output(&mut pair.server, first).expect("path fixture operation");
    assert!(Pair::path(&output, peer).next().is_some());
    let expired = first + Duration::from_secs(5);
    let output =
        dispatch::Capture::output(&mut pair.server, expired).expect("path fixture operation");
    assert!(output.iter().all(|packet| packet.addr() == CLIENT));
    pair.send_server(b"old-path-survives".to_vec(), expired)
        .expect("path fixture operation");
    let output = dispatch::Capture::output(&mut pair.server, expired + Duration::from_millis(1))
        .expect("path fixture operation");
    assert!(Pair::path(&output, CLIENT).next().is_some());
    let restart = expired + Duration::from_millis(2);
    pair.observe(peer, vec![0x44; 500], restart, restart)
        .expect("path fixture operation");
    let output =
        dispatch::Capture::output(&mut pair.server, restart).expect("path fixture operation");
    assert!(Pair::path(&output, peer).next().is_some());
    for router in [&pair.client, &pair.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}

#[test]
fn client_without_reset_keeps_only_its_initial_cid() {
    let options = Pending::options()
        .expect("transport options")
        .into_builder()
        .security(|security| security.reset(None))
        .try_build()
        .expect("reset is optional");
    let pending = Pending::try_with(0x39, 0x81, options).expect("client without reset");
    let mut pair = pending
        .establish()
        .expect("connection establishes without reset");
    let at = pair.start + Duration::from_millis(20);
    let local = Pair::access(&mut pair.client, pair.client_id, at, |conn| {
        conn.view()
            .path()
            .local_cids()
            .values()
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>()
    })
    .expect("client CID set");
    let [initial] = local.as_slice() else {
        panic!("reset None must retain exactly its issued initial CID");
    };

    let rebound = SocketAddr::from(([10, 0, 0, 1], 50_001));
    pair.observe(rebound, b"new-address".to_vec(), at, at)
        .expect("authenticated request on a new address");
    let output = dispatch::Capture::output(&mut pair.server, at).expect("path output");
    assert_eq!(
        pair.server.handler().datagrams,
        vec![b"new-address".to_vec()]
    );
    assert!(!output.is_empty());
    assert!(output.iter().all(|packet| packet.addr() == CLIENT));
    let mut observed = 0;
    for cid in output.iter().filter_map(Pair::short_cid) {
        assert_eq!(cid, initial);
        observed += 1;
    }
    assert_ne!(observed, 0);
    assert_eq!(
        Pair::peer(&mut pair.server, pair.server_id, at).expect("active path"),
        Some(CLIENT)
    );
    assert!(
        Pair::access(&mut pair.server, pair.server_id, at, |conn| conn
            .view()
            .lifecycle()
            .is_ready())
        .expect("missing peer CID does not close the connection")
    );
    for router in [&pair.client, &pair.server] {
        assert!(router.handler().errors.is_empty());
    }
}

#[test]
fn new_peer_cid_resumes_pending_path_validation() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let at = start + Duration::from_millis(20);
    let mut ping = [0; 1_200];
    ping[0] = frame::TYPE_PING;
    let mut request = peer.packet(&ping).expect("migration intent");
    Pair::receive(&mut server, target, &mut request, at).expect("pending candidate");
    let before = dispatch::Capture::output(&mut server, at).expect("old path validation");
    assert!(!before.is_empty());
    assert!(before.iter().all(|packet| packet.addr() == CLIENT));
    let initial = before
        .iter()
        .find_map(Pair::short_cid)
        .expect("actual initial CID on the old path")
        .to_vec();
    assert!(
        before
            .iter()
            .filter_map(Pair::short_cid)
            .all(|cid| cid == initial)
    );
    let mut old = paths::try_response(&mut peer, before, CLIENT).expect("old path response");
    Pair::receive(&mut server, CLIENT, &mut old, at).expect("old path remains reachable");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("waiting for a peer CID"),
        Some(CLIENT)
    );

    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("first unused CID"))
        .expect("NEW_CONNECTION_ID packet");
    Pair::receive(&mut server, target, &mut notice, at).expect("supply the missing CID");
    let resumed = dispatch::Capture::output(&mut server, at).expect("candidate validation");
    assert!(Pair::path(&resumed, target).next().is_some());
    assert!(Pair::path(&resumed, target).all(|packet| {
        Pair::short_cid(packet) == Some([0x59, 65, 0, 0, 0, 0, 0, 0].as_slice())
    }));
    assert_ne!(initial.as_slice(), [0x59, 65, 0, 0, 0, 0, 0, 0]);
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("challenge alone does not validate"),
        Some(CLIENT)
    );
    let mut response = paths::try_response(&mut peer, resumed, target).expect("candidate response");
    Pair::receive(&mut server, target, &mut response, at).expect("validated candidate");
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("same connection resumes"),
        Some(target)
    );
    dispatch::Capture::send(&mut server, handle, b"resumed".to_vec(), at)
        .expect("application output resumes");
    let send_at = Pair::access(&mut server, handle, at, |conn| {
        conn.view().recovery().send_at(at)
    })
    .expect("native send deadline");
    let output = dispatch::Capture::output(&mut server, send_at).expect("resumed output");
    let mut payloads = Vec::new();
    for mut packet in output {
        assert_eq!(packet.addr(), target);
        assert_eq!(
            Pair::short_cid(&packet),
            Some([0x59, 65, 0, 0, 0, 0, 0, 0].as_slice())
        );
        for segment in packet.segments_mut() {
            let Some(mut wire) = peer.receive(segment).expect("receive resumed output") else {
                continue;
            };
            let (_, body) = peer.open(&mut wire).expect("open resumed output");
            for record in frame::Frame::decode_all(&body).expect("resumed frames") {
                if let frame::View::Datagram { data, .. } = record.view() {
                    payloads.push(data.to_vec());
                }
            }
        }
    }
    assert_eq!(payloads, vec![b"resumed".to_vec()]);
    assert!(server.handler().errors.is_empty());
}

#[test]
fn retries_validation() {
    let pending = Pending::try_new(0x39, 0x81).expect("paths fixture must open");
    let mut pair = pending.establish().expect("path fixture operation");
    let peer = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let first = pair.start + Duration::from_millis(20);
    pair.observe(peer, vec![0x55; 500], first, first)
        .expect("path fixture operation");
    let output =
        dispatch::Capture::output(&mut pair.server, first).expect("path fixture operation");
    assert!(Pair::path(&output, peer).next().is_some());
    let (cursor, mut retry) =
        Pair::retry(&mut pair.server, first, 8, Some(peer)).expect("path fixture operation");
    assert!(Pair::path(&retry, peer).next().is_some());
    assert!(Pair::path(&retry, peer).all(|packet| packet.payload().len() >= 1_200));
    let at = pair
        .answer(&mut retry, peer, cursor)
        .expect("path fixture operation");
    pair.send_server(b"retry-validated".to_vec(), at)
        .expect("path fixture operation");
    let output = dispatch::Capture::output(&mut pair.server, at + Duration::from_millis(1))
        .expect("path fixture operation");
    assert!(Pair::path(&output, peer).next().is_some());
    for router in [&pair.client, &pair.server] {
        assert!(
            router.handler().errors.is_empty(),
            "{:?}",
            router.handler().errors
        );
    }
}

#[test]
fn separates_path_budgets() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let first = SocketAddr::from(([10, 0, 0, 1], 51_001));
    let second = SocketAddr::from(([10, 0, 0, 1], 51_002));
    let at = start + Duration::from_millis(20);
    for sequence in 65..68 {
        let mut notice = peer
            .packet(&bytes::notice(sequence, 0).expect("unused CID"))
            .expect("CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    }
    let token = [0x71, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x64, 0x31];
    let mut first_frames = vec![frame::TYPE_PATH_CHALLENGE];
    first_frames.extend(token);
    first_frames.push(frame::TYPE_DATAGRAM);
    first_frames.extend_from_slice(&[0x31; 500]);
    let mut request = peer.packet(&first_frames).expect("first path packet");
    let first_n = request.len();
    Pair::receive(&mut server, first, &mut request, at).expect("first path packet");
    let mut second_frames = vec![frame::TYPE_DATAGRAM];
    second_frames.extend_from_slice(&[0x32; 100]);
    let mut refused = peer.packet(&second_frames).expect("second path packet");
    let error = Pair::receive(&mut server, second, &mut refused, at)
        .expect_err("the first pending reply owns the only candidate CID");
    assert!(matches!(error, common::runtime::Error::Receive(
        dope_quic::errors::Receive::Packet(conn::Error::Path(
            conn::wire::path::Error::Address(conn::wire::path::Address::Cid { address })
        ))
    ) if address.addr() == second));
    assert!(
        Pair::access(&mut server, handle, at, |conn| conn
            .view()
            .lifecycle()
            .is_ready())
        .expect("the resource refusal keeps the healthy connection")
    );
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(CLIENT)
    );
    let mut flights =
        dispatch::Capture::output(&mut server, at).expect("publish the retained reply");
    assert_eq!(server.handler().datagrams, vec![vec![0x31; 500]]);
    assert!(Pair::path(&flights, second).next().is_none());
    let first_cid = Pair::path(&flights, first)
        .find_map(Pair::short_cid)
        .expect("first response CID")
        .to_vec();
    let active_cid = Pair::path(&flights, CLIENT)
        .find_map(Pair::short_cid)
        .expect("previous active path challenge CID")
        .to_vec();
    assert_ne!(first_cid, active_cid);
    let mut responses = 0;
    for packet in &mut flights {
        let selected = packet.addr() == first;
        for segment in packet.segments_mut() {
            let Some(mut wire) = peer.receive(segment).expect("receive output in order") else {
                continue;
            };
            let (_, body) = peer.open(&mut wire).expect("open first path output");
            for record in frame::Frame::decode_all(&body).expect("first path frames") {
                if let frame::View::PathResponse { data } = record.view()
                    && data == &token
                {
                    assert!(selected);
                    responses += 1;
                }
            }
        }
    }
    assert_eq!(responses, 1);
    let mut request = peer
        .packet(&second_frames)
        .expect("fresh second path packet");
    let second_n = request.len();
    Pair::receive(&mut server, second, &mut request, at)
        .expect("first reply publication releases the candidate owner");
    let second_flight = dispatch::Capture::output(&mut server, at).expect("second path validation");
    assert_eq!(
        server.handler().datagrams,
        vec![vec![0x31; 500], vec![0x32; 100]]
    );
    assert!(Pair::path(&second_flight, first).next().is_none());
    let second_cid = Pair::path(&second_flight, second)
        .find_map(Pair::short_cid)
        .expect("second candidate owns its CID");
    assert_ne!(second_cid, first_cid.as_slice());
    assert_ne!(second_cid, active_cid.as_slice());
    flights.extend(second_flight);
    let first_i = flights
        .iter()
        .position(|packet| packet.addr() == first)
        .expect("the retained PATH_RESPONSE must keep its original destination");
    let second_i = flights
        .iter()
        .position(|packet| packet.addr() == second)
        .expect("replacement validation must keep its independent destination");
    assert!(first_i < second_i);
    assert!(
        flights
            .iter()
            .filter(|packet| packet.addr() == first || packet.addr() == second)
            .all(|packet| packet.packet_count().get() == 1)
    );
    let sent_a = Pair::path(&flights, first)
        .map(|packet| packet.payload().len())
        .sum::<usize>();
    let sent_b = Pair::path(&flights, second)
        .map(|packet| packet.payload().len())
        .sum::<usize>();
    assert!(sent_a <= first_n.saturating_mul(3));
    assert!(sent_b <= second_n.saturating_mul(3));
    assert!(sent_a > second_n.saturating_mul(3));
    assert!(server.handler().errors.is_empty());
}

#[test]
fn responds_on_each_path() {
    let (mut peer, mut server, handle, start) = paths::try_wire().expect("wire path fixture");
    let first = SocketAddr::from(([10, 0, 0, 1], 52_001));
    let second = SocketAddr::from(([10, 0, 0, 1], 52_002));
    let token = [0x72, 0x65, 0x74, 0x72, 0x79, 0x2d, 0x6d, 0x65];
    let at = start + Duration::from_millis(20);
    for sequence in 65..68 {
        let mut notice = peer
            .packet(&bytes::notice(sequence, 0).expect("unused CID"))
            .expect("CID packet");
        Pair::receive(&mut server, CLIENT, &mut notice, at).expect("CID announcement");
    }
    let mut challenge = [0; 1_200];
    challenge[0] = frame::TYPE_PATH_CHALLENGE;
    challenge[1..9].copy_from_slice(&token);
    let mut request = peer.packet(&challenge).expect("first challenge packet");
    let first_n = request.len();
    Pair::receive(&mut server, first, &mut request, at).expect("first path challenge");
    let mut refused = peer.packet(&challenge).expect("second path packet");
    let error = Pair::receive(&mut server, second, &mut refused, at)
        .expect_err("the pending first reply retains its CID owner");
    assert!(matches!(error, common::runtime::Error::Receive(
        dope_quic::errors::Receive::Packet(conn::Error::Path(
            conn::wire::path::Error::Address(conn::wire::path::Address::Cid { address })
        ))
    ) if address.addr() == second));
    assert!(
        Pair::access(&mut server, handle, at, |conn| conn
            .view()
            .lifecycle()
            .is_ready())
        .expect("the resource refusal keeps the connection ready")
    );
    assert_eq!(
        Pair::peer(&mut server, handle, at).expect("active path"),
        Some(CLIENT)
    );

    let mut prior_cid: Option<Vec<u8>> = None;
    for source in [first, second] {
        let received = if source == first {
            first_n
        } else {
            let mut request = peer.packet(&challenge).expect("fresh second path packet");
            let received = request.len();
            Pair::receive(&mut server, source, &mut request, at)
                .expect("first reply publication releases the candidate owner");
            received
        };
        let flight = dispatch::Capture::output(&mut server, at).expect("exact path response");
        let cid = Pair::path(&flight, source)
            .find_map(Pair::short_cid)
            .expect("response CID")
            .to_vec();
        assert_ne!(prior_cid.as_deref(), Some(cid.as_slice()));
        let mut sent = 0;
        let mut responses = 0;
        for mut packet in flight {
            let selected = packet.addr() == source;
            if selected {
                sent += packet.payload().len();
                assert_eq!(Pair::short_cid(&packet), Some(cid.as_slice()));
                assert_eq!(packet.packet_count().get(), 1);
            } else {
                assert_eq!(packet.addr(), CLIENT);
                assert_ne!(Pair::short_cid(&packet), Some(cid.as_slice()));
            }
            for segment in packet.segments_mut() {
                let Some(mut wire) = peer.receive(segment).expect("receive output in order") else {
                    continue;
                };
                let (_, body) = peer.open(&mut wire).expect("open exact path output");
                for record in frame::Frame::decode_all(&body).expect("path frames") {
                    if let frame::View::PathResponse { data } = record.view()
                        && data == &token
                    {
                        assert!(selected);
                        responses += 1;
                    }
                }
            }
        }
        assert_eq!(responses, 1);
        assert!(sent <= received.saturating_mul(3));
        prior_cid = Some(cid);
    }
    assert!(server.handler().errors.is_empty());
}
