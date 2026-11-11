use common::peer::Config;
use common::peer::Pair;
use common::peer::Receiver;
use common::peer::Scope as _;
pub mod common;
use common::peer;
use peer::Bind;

use peer::Event;

use std::time::{Duration, Instant};

#[test]
fn fin_ignores_zero_credit() {
    let (mut server, mut client, mut work) = Pair::try_new(
        Config::new(0, 0, 1, 0)
            .try_build()
            .expect("fixture transport parameters"),
        Config::new(0, 0, 1, 0)
            .try_build()
            .expect("fixture transport parameters"),
    )
    .expect("connected peers")
    .into_parts();
    let stream = client
        .send_streams()
        .open_bidi()
        .expect("client can open the configured bidirectional stream");
    let peer = Bind::bind(stream, &server).expect("peer stream");
    client
        .send_streams()
        .finish(stream)
        .expect("client can finish the open stream");

    Receiver::receive_from(&mut server, &mut work, &mut client, Instant::now())
        .expect("peer packet transfer");

    assert!(
        server.poll().poll_event()
            == Some(Event::Readable {
                stream_id: peer.into()
            })
    );
    assert!(
        server
            .stream_state()
            .recv_eof(peer)
            .expect("peer stream remains addressable after FIN")
    );
}

#[test]
fn lost_fin_retransmits() {
    let (mut server, mut client, mut work) = Pair::try_new(
        Config::new(0, 0, 1, 0)
            .try_build()
            .expect("fixture transport parameters"),
        Config::new(0, 0, 1, 0)
            .try_build()
            .expect("fixture transport parameters"),
    )
    .expect("connected peers")
    .into_parts();
    let stream = client
        .send_streams()
        .open_bidi()
        .expect("client can open the configured bidirectional stream");
    let peer = Bind::bind(stream, &server).expect("peer stream");
    client
        .send_streams()
        .finish(stream)
        .expect("client can finish the open stream");
    let now = Instant::now();
    assert!(
        !{
            let (emitted_packets, send_result) =
                client.with_scope(|mut scope| scope.output().send(now));
            send_result.expect("warmed client journals must admit the original FIN");
            emitted_packets
        }
        .is_empty()
    );

    let timeout = client
        .view()
        .next_timer()
        .expect("FIN loss arms a recovery timer")
        + Duration::from_millis(1);
    assert_eq!(
        client.with_scope(|mut scope| scope.recovery().try_loss(timeout)),
        Ok(())
    );
    Receiver::receive_from(&mut server, &mut work, &mut client, timeout)
        .expect("peer packet transfer");

    assert!(
        server.poll().poll_event()
            == Some(Event::Readable {
                stream_id: peer.into()
            })
    );
}

#[test]
fn blocked_share_control() {
    use common::suite::peers::packet;
    use dope_quic::{
        conn::wire::packet::Batch,
        proto::param,
        wire::{frame, varint::VarInt},
    };

    for (data, stream) in [(0, 1024), (1024, 0)] {
        let clock = Instant::now();
        let params = param::Set::builder()
            .initial_data(data)
            .stream_uni(stream)
            .uni_count(3)
            .try_build()
            .expect("blocked credit");
        let mut pair = packet::Pair::try_params(1, params, clock, |_| {}).expect("native peer");
        pair.retired(clock).expect("settled handshake");
        for _ in 0..2 {
            let stream = pair.server.send_streams().open_uni().expect("stream");
            pair.server
                .send_streams()
                .send(stream, b"blocked")
                .expect("retained bytes");
        }
        let mut batch = Batch::default();
        let sent = clock + Duration::from_secs(1);
        pair.server
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, sent, 1, 1_200))
            .expect("lost BLOCKED carrier");
        assert_eq!(batch.packets(), 1);
        pair.peer
            .open(batch.iter_mut().next().expect("BLOCKED packet"))
            .expect("authenticated lost BLOCKED");
        pair.server
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, sent, 1, 1_200))
            .expect("park blocked owners");
        assert_eq!(batch.packets(), 0);

        let fin = pair
            .server
            .send_streams()
            .open_uni()
            .expect("independent FIN");
        pair.server.send_streams().finish(fin).expect("empty FIN");
        let later = sent + Duration::from_secs(1);
        pair.server
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, later, 1, 1_200))
            .expect("later carrier");
        let (number, _) = pair
            .peer
            .open(batch.iter_mut().next().expect("FIN packet"))
            .expect("authenticated FIN");
        let mut ack = Vec::new();
        frame::Borrowed::ack(
            VarInt::try_from(number).expect("packet number"),
            VarInt::ZERO,
            VarInt::ZERO,
            &[],
        )
        .expect("later ACK")
        .encode(&mut ack)
        .expect("encoded ACK");
        let now = later + Duration::from_millis(10);
        pair.receive(&ack, now).expect("lost BLOCKED feedback");
        pair.server
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, now, 1, 1_200))
            .expect("BLOCKED retry");
        let (_, body) = pair
            .peer
            .open(batch.iter_mut().next().expect("retry packet"))
            .expect("authenticated retry");
        let frames = frame::Frame::decode_all(&body).expect("retry frames");
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.view(), frame::View::DataBlocked { .. }))
                .count(),
            usize::from(data == 0)
        );
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.view(), frame::View::StreamDataBlocked { .. }))
                .count(),
            usize::from(stream == 0) * 2
        );
    }
}

#[test]
fn read_releases_credit() {
    let (mut server, mut client, mut work) = Pair::try_new(
        Config::new(5, 1 << 20, 8, 8)
            .try_build()
            .expect("fixture transport parameters"),
        Config::new(5, 1 << 20, 8, 8)
            .try_build()
            .expect("fixture transport parameters"),
    )
    .expect("connected peers")
    .into_parts();
    let stream = server
        .send_streams()
        .open_bidi()
        .expect("server can open the configured bidirectional stream");
    let peer = Bind::bind(stream, &client).expect("peer stream");
    server
        .send_streams()
        .send(stream, b"abcdefghij")
        .expect("server can queue data beyond current stream credit");
    let now = Instant::now();
    Receiver::receive_from(&mut client, &mut work, &mut server, now).expect("peer packet transfer");
    let mut received = Vec::new();
    client
        .recv()
        .recv(peer, &mut received)
        .expect("client can read the admitted prefix");
    assert_eq!(received, b"abcde");

    Receiver::receive_from(&mut server, &mut work, &mut client, now).expect("peer packet transfer");
    Receiver::receive_from(&mut client, &mut work, &mut server, now).expect("peer packet transfer");
    received.clear();
    client
        .recv()
        .recv(peer, &mut received)
        .expect("released credit admits the remaining bytes");
    assert_eq!(received, b"fghij");
}

#[test]
fn owned_releases_credit() {
    let (mut server, mut client, mut work) = Pair::try_new(
        Config::new(5, 1 << 20, 8, 8)
            .try_build()
            .expect("fixture transport parameters"),
        Config::new(5, 1 << 20, 8, 8)
            .try_build()
            .expect("fixture transport parameters"),
    )
    .expect("connected peers")
    .into_parts();
    let stream = server
        .send_streams()
        .open_bidi()
        .expect("server can open the configured bidirectional stream");
    let peer = Bind::bind(stream, &client).expect("peer stream");
    server
        .send_streams()
        .send(stream, b"abcdefghij")
        .expect("server can queue data beyond current stream credit");
    let now = Instant::now();
    Receiver::receive_from(&mut client, &mut work, &mut server, now).expect("peer packet transfer");

    assert_eq!(
        client
            .recv()
            .recv_owned(peer)
            .expect("client can move the admitted prefix")
            .as_deref(),
        Some(&b"abcde"[..])
    );
    assert!(
        client
            .recv()
            .recv_owned(peer)
            .expect("empty owned receive remains a valid stream read")
            .is_none()
    );

    Receiver::receive_from(&mut server, &mut work, &mut client, now).expect("peer packet transfer");
    Receiver::receive_from(&mut client, &mut work, &mut server, now).expect("peer packet transfer");
    assert_eq!(
        client
            .recv()
            .recv_owned(peer)
            .expect("released owned batch admits the remaining bytes")
            .as_deref(),
        Some(&b"fghij"[..])
    );
}

#[test]
fn released_credit_requires_publication() {
    use common::suite::peers::packet;
    use dope_quic::{conn, proto::param, wire::stream::Id};

    let now = Instant::now();
    let rejected = [(1, 2), (2, 1)].map(|(connection, stream)| {
        let params = param::Set::builder()
            .initial_data(connection)
            .stream_uni(stream)
            .uni_count(1)
            .try_build()
            .expect("bounded receive credit");
        let mut pair =
            packet::Pair::try_params(1, params, now, |_| {}).expect("authenticated packet peer");
        pair.receive(b"\x0a\x02\x01a", now)
            .expect("admitted first byte");
        let _ = pair
            .server
            .recv()
            .recv_owned(Id::new(2).expect("stream ID"))
            .expect("release received credit");
        matches!(
            pair.receive(b"\x0e\x02\x01\x01b", now),
            Err(packet::Error::Setup(peer::Setup::Server(
                peer::Transfer::Input(conn::Error::FlowControl)
            )))
        )
    });
    assert_eq!(rejected, [true; 2]);
}

#[test]
fn stop_freezes_stream_credit() {
    use common::suite::peers::packet;
    use dope_quic::{conn, errors::Code, proto::param, wire::stream::Id};

    let now = Instant::now();
    let params = param::Set::builder()
        .initial_data(8)
        .stream_uni(8)
        .uni_count(2)
        .try_build()
        .expect("two bounded receive streams");
    let mut pair =
        packet::Pair::try_params(2, params, now, |_| {}).expect("authenticated packet peer");
    let stopped = Id::new(2).expect("first stream ID");
    pair.receive(b"\x0a\x02\x03abc", now)
        .expect("first stream prefix");
    let held = pair
        .server
        .recv()
        .hold(stopped)
        .expect("receive stream")
        .expect("held prefix");
    pair.server
        .recv()
        .stop(stopped, Code::ZERO)
        .expect("stop receiving");
    pair.server
        .release_held(held)
        .map_err(|error| error.kind())
        .expect("return held connection credit");
    pair.server
        .recv()
        .stop(stopped, Code::ZERO)
        .expect("repeated stop preserves the admitted limit");

    pair.receive(b"\x0e\x02\x03\x05defgh", now)
        .expect("in-flight bytes within the original limit");
    assert_eq!(
        pair.server
            .recv()
            .recv_owned(stopped)
            .expect("discard the admitted in-flight bytes")
            .as_deref(),
        Some(&b"defgh"[..])
    );
    let now = pair.server.view().send_at(now + Duration::from_millis(30));
    pair.server
        .with_scope(|mut scope| scope.output().send(now))
        .1
        .expect("publish returned connection credit before ACK");
    pair.receive(b"\x0b\x06\x08progress", now)
        .expect("discarded bytes return connection credit for another stream");
    assert_eq!(
        pair.server
            .recv()
            .recv_owned(Id::new(6).expect("second stream ID"))
            .expect("other receive stream")
            .as_deref(),
        Some(&b"progress"[..])
    );
    assert!(matches!(
        pair.receive(b"\x0e\x02\x08\x01x", now),
        Err(packet::Error::Setup(peer::Setup::Server(
            peer::Transfer::Input(conn::Error::FlowControl)
        )))
    ));
}
