use common::suite::mux::dispatch;
pub mod common;

use common::peer::Pair;
use common::suite::{mux::retry, retry::Capture};
use dope_quic::conn::{self, life::options::Options};
use dope_quic::runtime::mux::builder::Plan;
use dope_quic::{security::crypto, wire::frame};
use std::{net::SocketAddr, pin::Pin, time::Instant};

#[test]
fn advertises_srt() {
    let secret = [0xA5u8; 32];
    let (_, client, _) = Pair::try_new(
        retry::try_config(Some(secret)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();

    let peer_tp = client
        .view()
        .peer()
        .params()
        .expect("peer TP after handshake");
    let token = peer_tp
        .identity()
        .reset()
        .expect("server should advertise stateless_reset_token");
    assert_ne!(token, [0u8; 16], "token must not be all zeros");
}

#[test]
fn omits_srt() {
    let (_, client, _) = Pair::try_new(
        retry::try_config(None).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();
    let peer_tp = client.view().peer().params().expect("peer TP");
    assert!(
        peer_tp.identity().reset().is_none(),
        "no secret ⇒ no advertised SRT",
    );
}

#[test]
fn srt_is_stable() {
    let secret = [0xBBu8; 32];
    let (_, client_a, _) = Pair::try_new(
        retry::try_config(Some(secret)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();
    let (_, client_b, _) = Pair::try_new(
        retry::try_config(Some(secret)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();

    let token_a = client_a
        .view()
        .peer()
        .params()
        .expect("first peer transport parameters must exist after handshake")
        .identity()
        .reset()
        .expect("first server must advertise its reset token");
    let token_b = client_b
        .view()
        .peer()
        .params()
        .expect("second peer transport parameters must exist after handshake")
        .identity()
        .reset()
        .expect("second server must advertise its reset token");
    assert_eq!(token_a, token_b, "deterministic SRT must survive restart");
}

#[test]
fn matching_srt_closes() {
    for first in [0x40, 0x00, 0x80, 0xC0, 0xE0, 0xF0] {
        let (_, mut client, mut work) = Pair::try_new(
            retry::try_config(Some([0xC1; 32])).expect("reset options"),
            retry::try_config(None).expect("reset options"),
        )
        .expect("connected peers")
        .into_parts();
        let token = client
            .view()
            .peer()
            .params()
            .and_then(|params| params.identity().reset())
            .expect("peer reset token");
        let mut reset = if first == 0xE0 {
            let mut wire = retry::handshake();
            wire.resize(48, 0);
            wire[23] = 24;
            *wire.last_chunk_mut::<16>().expect("reset tail") = token;
            wire
        } else {
            retry::try_tail(first, 30, token).expect("reset tail")
        };
        client
            .with_scope(|mut scope| {
                scope.input(&mut work).recv_packet(
                    &mut reset,
                    Instant::now(),
                    |_: &rustls::Error| {},
                )
            })
            .expect("matching reset");
        assert!(client.view().path().is_reset(), "header {first:#x}");
        assert_eq!(client.view().lifecycle().state(), conn::State::Draining);
    }
}

#[test]
fn authenticated_packet_with_reset_tail_keeps_its_outcome() {
    use common::suite::peers::packet::{Pair, bytes};

    for kind in [frame::TYPE_PING, 0x1f] {
        let now = Instant::now();
        let mut pair = Pair::try_new(1, now, |_| {}).expect("packet peer");
        let mut packet = pair.peer.packet(&[kind]).expect("peer Ping");
        let mut notice = bytes::notice(1, 1).expect("replacement CID");
        *notice.last_chunk_mut::<16>().expect("notice token") =
            *packet.last_chunk::<16>().expect("packet tag");
        pair.receive(&notice, now).expect("install reset token");
        assert_eq!(pair.retired(now).expect("use replacement CID"), [0]);

        let result = pair.server.with_scope(|mut scope| {
            scope
                .input(&mut pair.work)
                .recv_packet(&mut packet, now, |_| {})
        });
        if kind == frame::TYPE_PING {
            result.expect("earlier authenticated Ping");
            assert!(pair.server.view().lifecycle().is_ready());
        } else {
            assert!(matches!(result, Err(conn::Error::Frame(_))));
            assert_eq!(pair.server.view().lifecycle().state(), conn::State::Closing);
        }
        assert!(!pair.server.view().path().is_reset());
    }
}

#[test]
fn unknown_tail_ignored() {
    let secret = [0xC2u8; 32];
    let (_, mut client, mut work) = Pair::try_new(
        retry::try_config(Some(secret)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();

    let mut reset = retry::try_tail(0x40, 30, retry::UNKNOWN_TOKEN).expect("reset fixture tail");
    let now = Instant::now();

    let result = client.with_scope(|mut scope| {
        scope
            .input(&mut work)
            .recv_packet(&mut reset, now, |_: &rustls::Error| {})
    });
    assert!(matches!(
        result,
        Err(conn::Error::Crypto(crypto::Error::Decrypt(
            rustls::Error::DecryptError
        )))
    ));
    assert!(
        !client.view().path().is_reset(),
        "random tail must not trigger reset",
    );
    assert!(client.view().lifecycle().is_ready(), "conn stays open");
}

#[test]
fn pre_handshake_ignored() {
    let mut client = retry::try_fresh().expect("fresh connection");
    let mut work = conn::Workspace::try_new().expect("receive workspace allocation");
    let mut reset = retry::try_tail(0x40, 30, [0xFF; 16]).expect("reset fixture tail");
    let result = client.with_scope(|mut scope| {
        scope
            .input(&mut work)
            .recv_packet(&mut reset, Instant::now(), |_: &rustls::Error| {})
    });
    assert!(result.is_ok());
    assert!(!client.view().path().is_reset());
}

#[test]
fn unknown_dcid_emits_reset() {
    let secret = [0xD1u8; 32];
    let mut mux = retry::try_reset(secret, Plan::standard()).expect("reset server");

    let dcid = [0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89];
    let mut wire = retry::short(&dcid, 30).expect("short packet fixture");

    let from: SocketAddr = "127.0.0.1:55555"
        .parse()
        .expect("test peer address must be valid");
    Capture::receive(&mut mux, from, &mut wire, Instant::now())
        .expect("server must process the unknown short-header packet");

    let outgoing = dispatch::Capture::output(&mut mux, Instant::now()).expect("retry output");
    assert_eq!(outgoing.len(), 1, "exactly one reset should be emitted");
    let (dst, reset) = (outgoing[0].addr(), outgoing[0].payload());
    assert_eq!(dst, from, "reset goes back to triggering address");
    assert!(
        reset.len() >= 22 && reset.len() < wire.len(),
        "reset must be 22..{} bytes, got {}",
        wire.len(),
        reset.len()
    );

    assert_eq!(reset[0] & 0xC0, 0x40);

    let mut tail = [0u8; 16];
    tail.copy_from_slice(&reset[reset.len() - 16..]);
    assert_ne!(tail, [0u8; 16], "tail must be non-zero (real SRT)");

    let mut wire2 = retry::short(&dcid, 30).expect("short packet fixture");
    Capture::receive(&mut mux, from, &mut wire2, Instant::now())
        .expect("server must process the repeated unknown packet");
    let again = dispatch::Capture::output(&mut mux, Instant::now()).expect("retry output");
    let mut tail2 = [0u8; 16];
    let p2 = again[0].payload();
    tail2.copy_from_slice(&p2[p2.len() - 16..]);
    assert_eq!(tail, tail2, "SRT must be deterministic per (secret, dcid)");
}

#[test]
fn reset_respects_ceiling() {
    let limits = Plan::try_new(Plan::standard().connections(), 1, 22)
        .expect("stateless-reset fixture limits are valid");
    let mut mux = retry::try_reset([0xd4; 32], limits).expect("reset server");
    let mut wire = retry::short(&[0x99; 8], 64).expect("short packet fixture");
    Capture::receive(
        &mut mux,
        "127.0.0.1:4444"
            .parse()
            .expect("test peer address must be valid"),
        &mut wire,
        Instant::now(),
    )
    .expect("server must process the unknown short-header packet");
    let reset = Capture::take(&mut mux, Instant::now())
        .expect("retry take")
        .expect("unknown short-header packet must emit one reset");
    assert_eq!(reset.payload().len(), 22);
    assert_eq!(Pin::new(&mut mux).output().bytes(), 0);
}

#[test]
fn no_secret_no_reset() {
    let mut mux = retry::try_server(Options::default(), Plan::standard()).expect("server fixture");

    let dcid = [0x11u8; 8];
    let mut wire = retry::short(&dcid, 30).expect("short packet fixture");

    let from: SocketAddr = "127.0.0.1:1"
        .parse()
        .expect("test peer address must be valid");
    Capture::receive(&mut mux, from, &mut wire, Instant::now())
        .expect("server must process the unknown short-header packet");
    assert!(
        Capture::take(&mut mux, Instant::now())
            .expect("retry take")
            .is_none(),
        "no secret ⇒ no reset"
    );
}

#[test]
fn initial_no_reset() {
    let secret = [0xD2u8; 32];
    let mut mux = retry::try_reset(secret, Plan::standard()).expect("reset server");

    let mut wire = vec![0xC0u8; 30];
    let from: SocketAddr = "127.0.0.1:2"
        .parse()
        .expect("test peer address must be valid");
    assert!(Capture::receive(&mut mux, from, &mut wire, Instant::now()).is_ok());
    assert!(
        Capture::take(&mut mux, Instant::now())
            .expect("retry take")
            .is_none(),
        "Initial path must not emit reset"
    );
}

/// Unknown long headers do not cause an outgoing reset.
#[test]
fn long_header_no_reset() {
    let secret = [0xD5u8; 32];
    let mut mux = retry::try_reset(secret, Plan::standard()).expect("reset server");

    let mut wire = retry::handshake();
    let from: SocketAddr = "127.0.0.1:3"
        .parse()
        .expect("test peer address must be valid");

    Capture::receive(&mut mux, from, &mut wire, Instant::now())
        .expect("server must process the unknown long-header packet");
    assert!(
        Capture::take(&mut mux, Instant::now())
            .expect("retry take")
            .is_none(),
        "an unroutable long-header packet is silently discarded"
    );
}

#[test]
fn restart_recovers() {
    let secret = [0xD3u8; 32];
    let (_, mut client, mut work) = Pair::try_new(
        retry::try_config(Some(secret)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();
    client
        .with_scope(|mut scope| scope.datagrams().try_send(vec![0]))
        .expect("established test connection must accept a DATAGRAM");
    let release = client.view().recovery().send_at(Instant::now());
    let (emitted_packets, send_result) =
        client.with_scope(|mut scope| scope.output().send(release));
    send_result.expect("warmed connection journals must admit the application packet");
    let packet = emitted_packets
        .into_iter()
        .next()
        .expect("application packet");
    let dcid = &packet[1..9];

    let mut server_b = retry::try_reset(secret, Plan::standard()).expect("reset server");
    let mut wire = retry::short(dcid, 30).expect("short packet fixture");
    let from: SocketAddr = "127.0.0.1:33333"
        .parse()
        .expect("test peer address must be valid");
    Capture::receive(&mut server_b, from, &mut wire, Instant::now())
        .expect("restarted server must process the unknown short-header packet");
    let mut outgoing =
        dispatch::Capture::output(&mut server_b, Instant::now()).expect("retry output");
    assert_eq!(outgoing.len(), 1);
    let reset = outgoing[0].payload_mut();

    client
        .with_scope(|mut scope| {
            scope
                .input(&mut work)
                .recv_packet(reset, Instant::now(), |_: &rustls::Error| {})
        })
        .expect("client must receive the restarted server reset");
    assert!(
        client.view().path().is_reset(),
        "client should accept the reset"
    );
    assert_eq!(
        client.view().lifecycle().state(),
        dope_quic::conn::State::Draining
    );
}

#[test]
fn secrets_differ() {
    let secret_a = [0x01u8; 32];
    let secret_b = [0x02u8; 32];
    let (_, client_a, _) = Pair::try_new(
        retry::try_config(Some(secret_a)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();
    let (_, client_b, _) = Pair::try_new(
        retry::try_config(Some(secret_b)).expect("reset options"),
        retry::try_config(None).expect("reset options"),
    )
    .expect("connected peers")
    .into_parts();
    let token_a = client_a
        .view()
        .peer()
        .params()
        .expect("first peer transport parameters must exist after handshake")
        .identity()
        .reset()
        .expect("first server must advertise its reset token");
    let token_b = client_b
        .view()
        .peer()
        .params()
        .expect("second peer transport parameters must exist after handshake")
        .identity()
        .reset()
        .expect("second server must advertise its reset token");
    assert_ne!(token_a, token_b);
}
