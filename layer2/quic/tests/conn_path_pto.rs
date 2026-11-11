pub mod common;

use common::suite::mux::{
    dispatch,
    paths::{self, CLIENT, Pair},
};
use common::suite::peers::packet::bytes;
use dope_quic::wire::frame;
use std::{net::SocketAddr, time::Duration};

#[test]
fn candidate_preserves_pto() {
    let (mut peer, mut server, handle, mut at) = paths::try_wire().expect("wire path fixture");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let mut notice = peer
        .packet(&bytes::notice(65, 0).expect("spare CID"))
        .expect("spare CID packet");
    Pair::receive(&mut server, CLIENT, &mut notice, at).expect("spare CID announcement");
    let stream = Pair::access(&mut server, handle, at, |mut conn| {
        let mut streams = conn.streams();
        let mut send = streams.send();
        let id = send.open_uni().expect("stream credit");
        send.send(id, &[b'p'; 32_768]).expect("stream payload");
        send.finish(id).expect("stream FIN");
        id.id()
    })
    .expect("live connection");

    for _ in 0..32 {
        let (sent, window, ready, mtu) = Pair::access(&mut server, handle, at, |conn| {
            let view = conn.view();
            let state = view.recovery();
            (
                state.in_flight(),
                state.window(),
                view.send_at(at),
                view.path().mtu(),
            )
        })
        .expect("congestion state");
        if sent <= window && window - sent < mtu {
            break;
        }
        at = ready;
        let flight = dispatch::Capture::output(&mut server, at).expect("fill congestion window");
        for mut packet in flight {
            for segment in packet.segments_mut() {
                if let Some(mut wire) = peer.receive(segment).expect("receive stream packet") {
                    peer.open(&mut wire).expect("open stream packet");
                }
            }
        }
    }
    let (sent, window, due, mtu) = Pair::access(&mut server, handle, at, |conn| {
        let view = conn.view();
        let state = view.recovery();
        (
            state.in_flight(),
            state.window(),
            view.next_timer().expect("unacknowledged stream PTO"),
            view.path().mtu() as usize,
        )
    })
    .expect("full congestion state");
    assert!(
        sent <= window && window - sent < mtu as u64,
        "flight {sent} must leave less than the next MTU in window {window}",
    );
    assert!(due > at && due < at + Duration::from_secs(30));

    at += Duration::from_micros(1);
    assert!(at < due);
    let mut request = vec![frame::TYPE_PADDING; mtu];
    request[0] = frame::TYPE_PATH_CHALLENGE;
    request[1..9].copy_from_slice(b"pto-path");
    let mut wire = peer.packet(&request).expect("candidate challenge");
    let credit = wire.len() * 3;
    assert!(credit >= mtu, "candidate credit must fit one full packet");
    Pair::receive(&mut server, target, &mut wire, at).expect("candidate packet");
    let flight = dispatch::Capture::output(&mut server, at).expect("candidate staging");
    for mut packet in flight {
        let selected = packet.addr() == target;
        for segment in packet.segments_mut() {
            if let Some(mut wire) = peer.receive(segment).expect("receive staged packet") {
                peer.open(&mut wire).expect("open staged packet");
            }
            assert!(!selected, "full window must retain the candidate response");
        }
    }
    let (sent, window, timer) = Pair::access(&mut server, handle, at, |conn| {
        let view = conn.view();
        let state = view.recovery();
        (state.in_flight(), state.window(), view.next_timer())
    })
    .expect("staged congestion state");
    assert!(sent <= window && window - sent < mtu as u64);
    assert_eq!(timer, Some(due), "candidate must preserve the active PTO");

    let fired = due + Duration::from_nanos(1);
    let flight = dispatch::Capture::output(&mut server, fired).expect("active PTO output");
    let mut found = false;
    for mut packet in flight {
        let selected = packet.addr() == CLIENT;
        for segment in packet.segments_mut() {
            let Some(mut wire) = peer.receive(segment).expect("receive PTO packet") else {
                continue;
            };
            let (_, frames) = peer.open(&mut wire).expect("open PTO packet");
            for frame in frame::Frame::decode_all(&frames).expect("PTO frames") {
                found |= selected
                    && match frame.view() {
                        frame::View::Ping => true,
                        frame::View::Stream {
                            stream_id: id,
                            data,
                            ..
                        } => id == stream && !data.is_empty(),
                        _ => false,
                    };
            }
        }
    }
    assert!(
        found,
        "full window must still emit active PING or STREAM PTO"
    );
    assert!(server.handler().errors.is_empty());
}

#[test]
fn candidate_preserves_journal() {
    use common::runtime::gso::{self, Capture};
    use common::suite::peers::config::Parameters as _;
    use dope_quic::conn::life::options::Options;
    use std::pin::Pin;

    let mut pair = gso::Pair::new(|| {
        Options::try_journal()
            .expect("two packet slots")
            .into_builder()
            .security(|security| security.reset(Some([0x51; 32])))
            .try_build()
            .expect("spare connection IDs")
    })
    .expect("connected peers");
    let server = pair.server.sole().expect("server connection");
    let at = pair.now + Duration::from_secs(1);
    Capture::send(&mut pair.server, server, vec![0x31; 1_150], at).expect("active datagram");
    let held = Capture::output(&mut pair.server, at).expect("active flight");
    assert_eq!(held.len(), 1);
    let due = pair
        .server
        .conn(server)
        .expect("active connection")
        .view()
        .next_timer()
        .expect("active PTO");
    let target = SocketAddr::from(([10, 0, 0, 1], 50_001));
    let token = *b"pto-path";
    Capture::access(&mut pair.client, pair.handle, at, |mut conn| {
        conn.path().try_probe(token)
    })
    .expect("client connection")
    .expect("path challenge");
    let challenge = Capture::output(&mut pair.client, at).expect("client challenge");
    Capture::deliver_at(&mut pair.server, target, challenge, at).expect("candidate ingress");
    let at = pair
        .server
        .conn(server)
        .expect("candidate connection")
        .view()
        .send_at(at + Duration::from_millis(1));
    assert!(at < due);
    let flight = Capture::output(&mut pair.server, at).expect("journal pressure defers candidate");
    assert!(flight.iter().all(|packet| packet.addr() != target));
    Capture::deliver_at(&mut pair.client, gso::SERVER, flight, at).expect("active ACK delivery");
    assert!(
        Pin::new(&mut pair.server)
            .observe(at, |turn| turn.deadline())
            .is_none_or(|deadline| deadline > at),
        "journal pressure must wait for recovery or ingress",
    );

    let fired = due + Duration::from_nanos(1);
    let probe = Capture::output(&mut pair.server, fired).expect("reserved PTO slot");
    assert!(probe.iter().any(|packet| packet.addr() == gso::CLIENT));
    Capture::deliver_at(&mut pair.client, gso::SERVER, held, fired).expect("active delivery");
    Capture::deliver_at(&mut pair.client, gso::SERVER, probe, fired).expect("PTO delivery");
    let mut resumed = false;
    let mut at = fired;
    for _ in 0..4 {
        at = Pin::new(&mut pair.client)
            .observe(at, |turn| turn.deadline())
            .expect("peer ACK deadline")
            .max(at);
        let ack = Capture::output(&mut pair.client, at).expect("capacity-restoring ACK");
        Capture::deliver_at(&mut pair.server, gso::CLIENT, ack, at).expect("ACK ingress");
        at = pair
            .server
            .conn(server)
            .expect("acknowledged connection")
            .view()
            .send_at(at);
        let flight = Capture::output(&mut pair.server, at).expect("pending output");
        resumed |= flight.iter().any(|packet| packet.addr() == target);
        Capture::deliver_at(&mut pair.client, gso::SERVER, flight, at).expect("output delivery");
        if resumed {
            break;
        }
    }
    assert!(resumed, "the original candidate response must resume");
    assert!(
        pair.client
            .conn(pair.handle)
            .expect("client connection")
            .view()
            .path()
            .has_token(&token),
    );
}
