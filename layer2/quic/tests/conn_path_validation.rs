pub mod common;

use common::peer::Receiver as _;
use common::suite::peers::path;
use dope_quic::conn;
use std::time::{Duration, Instant};

#[test]
fn challenge_round_trip() {
    use dope_quic::wire::frame::{self, Frame, View};
    let data = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
    let mut buf = vec![frame::TYPE_PATH_CHALLENGE];
    buf.extend_from_slice(&data);
    let (decoded, n) = Frame::decode(&buf).expect("test PATH_CHALLENGE frame must decode");
    assert!(matches!(decoded.view(), View::PathChallenge { data: found } if found == &data));
    assert_eq!(n, buf.len());
}

#[test]
fn path_response_round_trip() {
    use dope_quic::wire::frame::{self, Frame, View};
    let data = [0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10];
    let mut buf = vec![frame::TYPE_PATH_RESPONSE];
    buf.extend_from_slice(&data);
    let (decoded, n) = Frame::decode(&buf).expect("test PATH_RESPONSE frame must decode");
    assert!(matches!(decoded.view(), View::PathResponse { data: found } if found == &data));
    assert_eq!(n, buf.len());
}

#[test]
fn challenge_validates_path() {
    let (mut server, mut client, mut work) = path::try_pair::<8_192>()
        .expect("path-validation peers")
        .into_parts();
    assert!(client.view().lifecycle().is_ready() && server.view().lifecycle().is_ready());
    let token = [0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD, 0xBE, 0xEF];

    assert_eq!(
        server.with_scope(|mut scope| scope.path().try_probe(token)),
        Ok(())
    );
    assert_eq!(
        server.with_scope(|mut scope| scope.path().try_probe(token)),
        Err(conn::wire::path::probe::Error::Duplicate { data: token })
    );
    assert!(
        !server.view().path().has_token(&token),
        "not yet validated — RESPONSE hasn't come back",
    );

    client
        .receive_from(&mut work, &mut server, Instant::now())
        .expect("peer packet transfer");
    server
        .receive_from(&mut work, &mut client, Instant::now())
        .expect("peer packet transfer");

    assert!(
        server.view().path().has_token(&token),
        "server should have validated the path after RESPONSE round-trip",
    );
}

#[test]
fn unsolicited_is_invalid() {
    let (server, client, _work) = path::try_pair::<8_192>()
        .expect("path-validation peers")
        .into_parts();
    assert!(client.view().lifecycle().is_ready() && server.view().lifecycle().is_ready());
    assert!(
        !server.view().path().has_token(&[0xFFu8; 8]),
        "never-issued token must not be reported validated",
    );
}

#[test]
fn pre_handshake_is_noop() {
    let mut client = path::try_initial().expect("initial path-validation client");
    assert!(client.view().lifecycle().is_opening());
    let token = [0x01u8; 8];
    assert_eq!(
        client.with_scope(|mut scope| scope.path().try_probe(token)),
        Err(conn::wire::path::probe::Error::State {
            state: conn::State::Handshaking,
            data: token
        })
    );
    let (flight, send_result) = client.with_scope(|mut scope| scope.output().send(Instant::now()));
    send_result.expect("initial handshake output remains available after rejection");
    assert!(!flight.is_empty(), "the ClientHello is still pending");
    assert!(flight.iter().all(|packet| packet[0] & 0x80 != 0));
    assert!(client.view().lifecycle().is_opening());
    assert!(!client.view().path().has_token(&token));
}

#[test]
fn challenges_independent() {
    let (mut server, mut client, mut work) = path::try_pair::<8_192>()
        .expect("path-validation peers")
        .into_parts();
    assert!(client.view().lifecycle().is_ready() && server.view().lifecycle().is_ready());
    let a = [0xAAu8; 8];
    let b = [0xBBu8; 8];
    assert_eq!(
        server.with_scope(|mut scope| scope.path().try_probe(a)),
        Ok(())
    );
    assert_eq!(
        server.with_scope(|mut scope| scope.path().try_probe(b)),
        Ok(())
    );
    client
        .receive_from(&mut work, &mut server, Instant::now())
        .expect("peer packet transfer");
    server
        .receive_from(&mut work, &mut client, Instant::now())
        .expect("peer packet transfer");
    assert!(server.view().path().has_token(&a));
    assert!(server.view().path().has_token(&b));
}

#[test]
fn backpressure_keeps() {
    const TOKENS: usize = 64;
    const CONTROL_CAPACITY: usize = 32;
    let (mut server, mut client, mut work) = path::try_pair::<CONTROL_CAPACITY>()
        .expect("bounded path-validation peers")
        .into_parts();
    assert!(client.view().lifecycle().is_ready() && server.view().lifecycle().is_ready());
    let cli_set: Vec<_> = (0..TOKENS)
        .map(|token| (token as u64).to_ne_bytes())
        .collect();
    let srv_set: Vec<_> = (TOKENS..TOKENS * 2)
        .map(|token| (token as u64).to_ne_bytes())
        .collect();

    for &token in &cli_set {
        assert_eq!(
            client.with_scope(|mut scope| scope.path().try_probe(token)),
            Ok(())
        );
    }
    for &token in &srv_set {
        assert_eq!(
            server.with_scope(|mut scope| scope.path().try_probe(token)),
            Ok(())
        );
    }

    for _ in 0..32 {
        server
            .receive_from(&mut work, &mut client, Instant::now())
            .expect("peer packet transfer");
        client
            .receive_from(&mut work, &mut server, Instant::now())
            .expect("peer packet transfer");
        server
            .receive_from(&mut work, &mut client, Instant::now())
            .expect("peer packet transfer");
        client
            .receive_from(&mut work, &mut server, Instant::now())
            .expect("peer packet transfer");
        if cli_set
            .iter()
            .all(|token| client.view().path().has_token(token))
            && srv_set
                .iter()
                .all(|token| server.view().path().has_token(token))
        {
            break;
        }
    }

    assert!(
        cli_set
            .iter()
            .all(|token| client.view().path().has_token(token)),
        "client challenges and server responses survive control pressure",
    );
    assert!(
        srv_set
            .iter()
            .all(|token| server.view().path().has_token(token)),
        "server challenges and client responses survive control pressure",
    );
}

#[test]
fn cancel_releases_probe_capacity() {
    use common::suite::peers::packet;
    use dope_quic::{
        conn::wire::{packet::Batch, path::probe},
        wire::{frame, varint::VarInt},
    };
    use o3::depot::capacity;

    let clock = Instant::now();
    let mut pair = packet::Pair::try_new(1, clock, |_| {}).expect("independent peer");
    pair.retired(clock).expect("settled handshake");
    for value in 0..64_u64 {
        pair.server
            .with_scope(|mut scope| scope.path().try_probe(value.to_ne_bytes()))
            .expect("bounded application probes");
    }
    let next = 64_u64.to_ne_bytes();
    assert_eq!(
        pair.server
            .with_scope(|mut scope| scope.path().try_probe(next)),
        Err(probe::Error::Capacity {
            data: next,
            reason: capacity::Reason::Full,
        })
    );
    let unsent = 0_u64.to_ne_bytes();
    assert!(
        pair.server
            .with_scope(|mut scope| scope.path().cancel_probe(unsent))
    );
    assert!(
        !pair
            .server
            .with_scope(|mut scope| scope.path().cancel_probe(unsent))
    );
    pair.server
        .with_scope(|mut scope| scope.path().try_probe(next))
        .expect("canceled pending probe releases capacity");

    let now = pair.server.view().send_at(clock + Duration::from_secs(1));
    let mut batch = Batch::default();
    pair.server
        .with_scope(|mut scope| scope.output().send_batch(&mut batch, now, 1, 1_200))
        .expect("challenge carrier");
    assert_eq!(batch.packets(), 1);
    let (number, body) = pair
        .peer
        .open(batch.iter_mut().next().expect("one carrier"))
        .expect("authenticated challenges");
    let frames = frame::Frame::decode_all(&body).expect("challenge frames");
    let sent = frames
        .iter()
        .find_map(|item| match item.view() {
            frame::View::PathChallenge { data } => Some(*data),
            _ => None,
        })
        .expect("outstanding application probe");
    assert!(
        !frames.iter().any(
            |item| matches!(item.view(), frame::View::PathChallenge { data } if data == &unsent)
        )
    );
    let mut ack = Vec::new();
    frame::Borrowed::ack(
        VarInt::try_from(number).expect("packet number"),
        VarInt::ZERO,
        VarInt::ZERO,
        &[],
    )
    .expect("carrier receipt")
    .encode(&mut ack)
    .expect("encoded receipt");
    pair.receive(&ack, now).expect("ACK without PATH_RESPONSE");
    assert!(
        pair.server
            .with_scope(|mut scope| scope.path().cancel_probe(sent))
    );
    pair.server
        .with_scope(|mut scope| scope.path().try_probe(65_u64.to_ne_bytes()))
        .expect("canceled acknowledged probe releases capacity");
    let mut response = vec![frame::TYPE_PATH_RESPONSE];
    response.extend_from_slice(&sent);
    pair.receive(&response, now)
        .expect("delayed canceled response");
    assert!(!pair.server.view().path().has_token(&sent));
}

#[test]
fn stale_ack_keeps_slot() {
    use common::suite::peers::packet;
    use dope_quic::{
        conn::wire::packet::Batch,
        wire::{frame, varint::VarInt},
    };

    let clock = Instant::now();
    let mut pair = packet::Pair::try_new(1, clock, |_| {}).expect("independent peer");
    pair.retired(clock).expect("settled handshake");
    let first = [0x51; 8];
    let next = [0x52; 8];
    let mut original = None;
    let mut now = clock + Duration::from_secs(1);
    let mut batch = Batch::default();
    for token in [first, next] {
        pair.server
            .with_scope(|mut scope| scope.path().try_probe(token))
            .expect("queued challenge");
        now = pair.server.view().send_at(now);
        pair.server
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, now, 1, 1_200))
            .expect("challenge carrier");
        assert_eq!(batch.packets(), 1);
        let (number, body) = pair
            .peer
            .open(batch.iter_mut().next().expect("one carrier"))
            .expect("authenticated challenge");
        assert!(frame::Frame::decode_all(&body)
            .expect("challenge frames")
            .iter()
            .any(|frame| matches!(frame.view(), frame::View::PathChallenge { data } if data == &token)));
        if token == first {
            original = Some(number);
            let mut response = vec![frame::TYPE_PATH_RESPONSE];
            response.extend_from_slice(&first);
            pair.receive(&response, now)
                .expect("response without carrier ACK");
            assert!(pair.server.view().path().has_token(&first));
        }
        now += Duration::from_millis(1);
    }

    let mut ack = Vec::new();
    frame::Borrowed::ack(
        VarInt::try_from(original.expect("original carrier")).expect("packet number"),
        VarInt::ZERO,
        VarInt::ZERO,
        &[],
    )
    .expect("old receipt")
    .encode(&mut ack)
    .expect("encoded receipt");
    pair.receive(&ack, now)
        .expect("delayed original carrier ACK");
    assert!(!pair.server.view().path().has_token(&next));

    let now = pair.server.view().next_timer().expect("replacement PTO") + Duration::from_nanos(1);
    pair.server
        .with_scope(|mut scope| scope.recovery().try_loss(now))
        .expect("replacement timeout");
    let (packets, result) = pair.server.with_scope(|mut scope| scope.output().send(now));
    result.expect("replacement retransmission");
    let mut retransmitted = false;
    for mut packet in packets {
        let (_, body) = pair
            .peer
            .open(&mut packet)
            .expect("authenticated retransmission");
        retransmitted |= frame::Frame::decode_all(&body)
            .expect("retransmitted frames")
            .iter()
            .any(|frame| matches!(frame.view(), frame::View::PathChallenge { data } if data == &next));
    }
    assert!(
        retransmitted,
        "the replacement owner must survive the original carrier ACK"
    );
}
