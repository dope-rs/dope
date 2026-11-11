use crate::common::peer;
pub mod common;
use common::suite::cid::{Count, INITIAL};
use dope_quic::runtime::mux::builder::Builder;
use dope_quic::runtime::mux::builder::Plan;
use std::{
    net::SocketAddr,
    pin::Pin,
    time::{Duration, Instant},
};

#[test]
fn retransmits_retiring_advertisement() {
    use common::suite::mux::{
        dispatch,
        paths::{self, CLIENT, Pair},
    };
    use dope_quic::{
        conn::scope::path::Rotation,
        wire::{frame, varint::VarInt},
    };

    let (mut peer, mut server, handle, mut now) = paths::try_wire().expect("wire CID fixture");
    let lost = dispatch::Capture::output(&mut server, now).expect("initial CID advertisements");
    assert!(!lost.is_empty());
    for mut packet in lost {
        for segment in packet.segments_mut() {
            if let Some(mut wire) = peer.receive(segment).expect("discarded CID packet") {
                // Advance the fixture's authenticated packet sequence without
                // processing the lost advertisements or acknowledging them.
                peer.open(&mut wire).expect("authenticate discarded packet");
            }
        }
        Pin::new(&mut server).output().recycle(packet);
    }

    let mut watermark = 0;
    for step in 0_u8..4 {
        if step < 3 {
            assert!(matches!(
                Pin::new(&mut server).observe(now, |mut turn| turn.protocol().rotate_cid(handle)),
                Ok(Rotation::Queued)
            ));
            now = server
                .conn(handle)
                .expect("live connection")
                .view()
                .recovery()
                .send_at(now);
        } else {
            assert!(matches!(
                Pin::new(&mut server).observe(now, |mut turn| turn.protocol().rotate_cid(handle)),
                Ok(Rotation::Unavailable)
            ));
            now = server
                .conn(handle)
                .expect("live connection")
                .view()
                .recovery()
                .next_timer()
                .expect("unacknowledged advertisements arm PTO")
                + Duration::from_nanos(1);
        }

        let flight = dispatch::Capture::output(&mut server, now).expect("CID output");
        let mut advertised = Vec::new();
        for mut packet in flight {
            for segment in packet.segments_mut() {
                let Some(mut wire) = peer.receive(segment).expect("receive CID packet") else {
                    continue;
                };
                let (_, body) = peer.open(&mut wire).expect("authenticated CID packet");
                for frame in frame::Frame::decode_all(&body).expect("valid CID frames") {
                    if let frame::View::NewConnectionId { notice, .. } = frame.view() {
                        let mut encoded = Vec::new();
                        frame.encode(&mut encoded).expect("CID frame encoding");
                        let (sequence, length) =
                            VarInt::decode(&encoded[1..]).expect("CID sequence");
                        let (prior, _) =
                            VarInt::decode(&encoded[1 + length..]).expect("retirement floor");
                        watermark = watermark.max(prior.get());
                        advertised.push((sequence.get(), prior.get(), *notice.id()));
                    }
                }
            }
            Pin::new(&mut server).output().recycle(packet);
        }

        if step < 3 {
            let expected = u64::from(step) + 4;
            assert_eq!(
                advertised
                    .iter()
                    .map(|&(sequence, prior, _)| (sequence, prior))
                    .collect::<Vec<_>>(),
                [(expected, u64::from(step) + 1)],
                "CID 2 remains unknown while newer advertisements raise the floor",
            );
            peer.set_target(advertised[0].2);
        } else {
            assert_eq!(watermark, 3);
            assert!(
                advertised
                    .iter()
                    .any(|&(sequence, prior, _)| sequence == 2 && prior == 0),
                "PTO retains the retiring CID's original valid advertisement",
            );
        }
        if step == 2 {
            continue;
        }
        let sequence = step.min(2);
        let mut retire = peer
            .packet(&[frame::TYPE_RETIRE_CONNECTION_ID, sequence])
            .expect("retirement packet on a replacement CID");
        Pair::receive(&mut server, CLIENT, &mut retire, now).expect("peer retirement");
    }

    assert!(matches!(
        Pin::new(&mut server).observe(now, |mut turn| turn.protocol().rotate_cid(handle)),
        Ok(Rotation::Queued)
    ));
}

#[test]
fn replenishes_before_retirement_within_slot_limit() {
    use common::suite::mux::dispatch::Capture;
    use dope_quic::{conn::scope::path::Rotation, proto::param};

    for limit in [2_u64, 8] {
        let (signing, key) = peer::raw(0x39).expect("raw identity");
        let mut server = Count::server(signing.server().expect("server TLS"), Plan::standard())
            .expect("CID server");
        let mut client = Builder::client(Count::default())
            .build()
            .expect("CID client");
        let remote = SocketAddr::from(([10, 0, 0, 2], 443));
        let local = SocketAddr::from(([10, 0, 0, 1], 50_000));
        let started = Instant::now();
        let params = param::Set::builder()
            .idle_ms(30_000)
            .datagram(Some(65535))
            .cid_limit(limit)
            .try_build()
            .expect("peer CID limit");
        let options = Count::options()
            .expect("CID options")
            .into_builder()
            .transport(|path| path.params(params))
            .try_build()
            .expect("bounded CID options");
        let outbound =
            Capture::connect(&mut client, remote, key, options, INITIAL.to_vec(), started)
                .expect("CID connection");
        for turn in 0..8 {
            let now = started + Duration::from_millis(turn * 10);
            Count::relay_at(&mut client, &mut server, local, now).expect("client handshake");
            Count::relay_at(&mut server, &mut client, remote, now).expect("server handshake");
        }
        assert_eq!(server.handler().established, 1);
        assert_eq!(client.handler().established, 1);
        let inbound = server.handler().last_handle.expect("server handle");
        let initial = server
            .conn(inbound)
            .expect("live server")
            .view()
            .path()
            .local_cids()
            .entries()
            .map(|(sequence, _)| sequence.get())
            .collect::<Vec<_>>();
        assert_eq!(initial, (0..limit).collect::<Vec<_>>());

        let now = started + Duration::from_secs(1);
        assert!(matches!(
            Count::rotate(&mut server, inbound, now),
            Ok(Rotation::Queued)
        ));
        assert!(matches!(
            Count::rotate(&mut server, inbound, now),
            Ok(Rotation::Unavailable)
        ));
        let retained = server
            .conn(inbound)
            .expect("retiring route retained")
            .view()
            .path()
            .local_cids()
            .entries()
            .map(|(sequence, _)| sequence.get())
            .collect::<Vec<_>>();
        assert_eq!(
            retained,
            (0..(limit + 1).min(8)).collect::<Vec<_>>(),
            "a replacement uses a vacant slot while the retiring route stays registered"
        );
        let send_at = server
            .conn(inbound)
            .expect("live server")
            .view()
            .recovery()
            .send_at(now);
        let rotation = Count::relay_at(&mut server, &mut client, remote, send_at)
            .expect("authenticated rotation");
        assert!(!rotation.is_empty());
        assert!(
            client
                .conn(outbound)
                .expect("peer retains the connection")
                .view()
                .lifecycle()
                .is_ready(),
            "the advertisement respects the peer's active CID limit"
        );

        let retire_at = client
            .conn(outbound)
            .expect("live client")
            .view()
            .recovery()
            .send_at(send_at + Duration::from_millis(1));
        let retirement = Count::relay_at(&mut client, &mut server, local, retire_at)
            .expect("authenticated retirement");
        assert!(!retirement.is_empty());
        let mut remaining = server
            .conn(inbound)
            .expect("retirement preserves the connection")
            .view()
            .path()
            .local_cids()
            .entries()
            .map(|(sequence, _)| sequence.get())
            .collect::<Vec<_>>();
        remaining.sort_unstable();
        assert_eq!(remaining, (1..=limit).collect::<Vec<_>>());
    }
}

#[test]
fn reuses_rotated_slot() {
    let (signing, key) = peer::raw(0x39).expect("raw identity");
    let mut server =
        Count::server(signing.server().expect("server TLS"), Plan::standard()).expect("CID server");
    let mut client = (Builder::client(Count::default()).build())
        .expect("standard client router plan must be valid");
    let peer = SocketAddr::from(([10, 0, 0, 2], 443));
    let local = SocketAddr::from(([10, 0, 0, 1], 50000));
    let started = Instant::now();
    let outbound = (Count::connect(&mut client, peer, key, INITIAL.to_vec(), started))
        .expect("fixed client connection parameters must be valid");

    for turn in 0..8 {
        let now = started + Duration::from_millis(turn * 10);
        Count::relay_at(&mut client, &mut server, local, now).expect("CID relay_at");
        Count::relay_at(&mut server, &mut client, peer, now).expect("CID relay_at");
    }
    assert_eq!(server.handler().established, 1);
    assert_eq!(client.handler().established, 1);
    let inbound = (server.handler().last_handle)
        .expect("established server must record its connection handle");

    let old_at = started + Duration::from_secs(1);
    assert!(
        Count::send(&mut client, outbound, vec![0x10], old_at).is_ok(),
        "live client connection must accept the first datagram"
    );
    Pin::new(&mut client).observe(old_at, |mut turn| {
        assert!(
            turn.output().drive().is_ok(),
            "fixture client connections must admit the first CID packet"
        );
    });
    let mut flights: Vec<_> =
        Pin::new(&mut client).observe(old_at, |mut turn| turn.output().drain().collect());
    let mut live = (flights
        .iter_mut()
        .flat_map(|outgoing| outgoing.segments_mut())
        .find(|wire| Count::short_cid(wire).is_some())
        .map(|wire| wire.to_vec()))
    .expect("one packet uses the initial server CID");
    for outgoing in flights {
        Pin::new(&mut client).output().recycle(outgoing);
    }
    let old_dcid = (Count::short_cid(&live))
        .expect("selected initial packet must expose its short destination CID");

    let queue_at = old_at + Duration::from_millis(1);
    assert!(
        Count::send(&mut client, outbound, vec![0x11], queue_at).is_ok(),
        "live client connection must accept the held datagram"
    );
    let stale_at = match Pin::new(&mut client).observe(queue_at, |turn| turn.deadline()) {
        Some(deadline) => deadline.max(queue_at),
        None => queue_at,
    };
    Pin::new(&mut client).observe(stale_at, |mut turn| {
        assert!(
            turn.output().drive().is_ok(),
            "fixture client connections must admit the held CID packet"
        );
    });
    let mut flights: Vec<_> =
        Pin::new(&mut client).observe(stale_at, |mut turn| turn.output().drain().collect());
    let mut stale = (flights
        .iter_mut()
        .flat_map(|outgoing| outgoing.segments_mut())
        .find(|wire| Count::short_cid(wire).is_some())
        .map(|wire| wire.to_vec()))
    .expect("a second packet uses the initial server CID");
    for outgoing in flights {
        Pin::new(&mut client).output().recycle(outgoing);
    }
    assert_eq!(Count::short_cid(&stale), Some(old_dcid));

    let start = stale_at + Duration::from_millis(1);
    assert!(matches!(
        Count::rotate(&mut server, inbound, start),
        Ok(dope_quic::conn::scope::path::Rotation::Queued)
    ));
    assert!(
        matches!(
            Count::rotate(&mut server, inbound, start),
            Ok(dope_quic::conn::scope::path::Rotation::Unavailable)
        ),
        "a rotation remains singular until its retirement arrives",
    );
    let mut dropped = 0;
    for turn in 0..4 {
        let now = start + Duration::from_millis(turn * 10);
        Pin::new(&mut server).observe(now, |mut turn| {
            assert!(
                turn.output().drive().is_ok(),
                "fixture server connections must admit CID rotation output"
            );
        });
        dropped += Pin::new(&mut server).observe(now, |mut turn| turn.output().drain().count());
    }
    assert_ne!(dropped, 0, "the first rotation flight is dropped");

    let live_at = start + Duration::from_millis(31);
    assert!(
        Count::receive(&mut server, local, &mut live, live_at).is_ok(),
        "packet on the live initial route must be accepted"
    );
    Pin::new(&mut server).observe(live_at, |mut turn| {
        assert!(
            turn.output().drive().is_ok(),
            "fixture server connections must admit live-route output"
        );
    });
    assert_eq!(
        server.handler().datagrams,
        1,
        "the old route remains valid before its retirement arrives",
    );
    Pin::new(&mut server).observe(live_at, |mut turn| turn.output().drain().count());
    let deadline = (server
        .conn(inbound)
        .and_then(|conn| conn.view().recovery().next_timer()))
    .expect("lost rotation control arms PTO");
    let probe_at = deadline + Duration::from_millis(1);
    let rotation = Count::relay_at(&mut server, &mut client, peer, probe_at).expect("CID relay_at");
    assert!(!rotation.is_empty(), "PTO retransmits the rotation control");

    let retire = probe_at + Duration::from_millis(1);
    let wires = Count::relay_at(&mut client, &mut server, local, retire).expect("CID relay_at");
    let mut active = (Count::changed_cid(&wires, old_dcid)).expect("RETIRE uses a replacement CID");

    let fill_at = retire + Duration::from_millis(1);
    Count::relay_at(&mut server, &mut client, peer, fill_at).expect("CID relay_at");
    let conn =
        (server.conn(inbound)).expect("established server connection must survive CID retirement");
    assert_eq!(conn.view().path().local_cids().entries().count(), 4);
    assert!(
        conn.view()
            .path()
            .local_cids()
            .values()
            .all(|cid| cid != old_dcid),
        "the retired CID leaves the connection's local set",
    );
    assert_eq!(
        server.handler().datagrams,
        1,
        "control exchange does not replay the held application packet",
    );
    assert!(
        Count::receive(&mut server, local, &mut stale, fill_at).is_ok(),
        "packet on the retired route must reach router input"
    );
    Pin::new(&mut server).observe(fill_at, |mut turn| {
        assert!(
            turn.output().drive().is_ok(),
            "fixture server connections must admit retired-route output"
        );
    });
    assert_eq!(server.handler().datagrams, 1, "the retired route is gone");
    Pin::new(&mut server).observe(fill_at, |mut turn| turn.output().drain().count());

    let mut cycle_at = fill_at;
    for _ in 0..4 {
        let start = cycle_at + Duration::from_millis(100);
        let conn = (server.conn(inbound))
            .expect("established server connection must survive repeated CID rotation");
        let retiring = (conn
            .view()
            .path()
            .local_cids()
            .entries()
            .map(|entry| entry.0)
            .min())
        .expect("live server connection must own at least one local CID");
        assert!(matches!(
            Count::rotate(&mut server, inbound, start),
            Ok(dope_quic::conn::scope::path::Rotation::Queued)
        ));
        let mut done = None;
        let mut now = start;
        for _ in 0..16 {
            Count::relay_at(&mut server, &mut client, peer, now).expect("CID relay_at");
            let wires =
                Count::relay_at(&mut client, &mut server, local, now).expect("CID relay_at");
            if let Some(changed) = Count::changed_cid(&wires, active) {
                active = changed;
            }
            let conn = (server.conn(inbound))
                .expect("established server connection must remain during CID retirement");
            let retired = conn
                .view()
                .path()
                .local_cids()
                .entries()
                .all(|entry| entry.0 != retiring);
            if retired {
                done = Some(now);
                break;
            }
            let deadline = ([
                Pin::new(&mut server).observe(now, |turn| turn.deadline()),
                Pin::new(&mut client).observe(now, |turn| turn.deadline()),
                server
                    .conn(inbound)
                    .and_then(|conn| conn.view().recovery().next_timer()),
                client
                    .conn(outbound)
                    .and_then(|conn| conn.view().recovery().next_timer()),
            ]
            .into_iter()
            .flatten()
            .filter(|deadline| *deadline > now)
            .min())
            .expect("an incomplete CID rotation owns a future deadline");
            now = deadline;
        }
        let done = (done).expect("rotation reaches RETIRE_CONNECTION_ID");
        let fill_at = server
            .conn(inbound)
            .expect("retirement retains the replacement CIDs")
            .view()
            .recovery()
            .send_at(done);
        Count::relay_at(&mut server, &mut client, peer, fill_at).expect("CID relay_at");
        cycle_at = fill_at;
    }
    let conn =
        (server.conn(inbound)).expect("established server connection must survive slot reuse");
    assert!(
        conn.view()
            .path()
            .local_cids()
            .entries()
            .any(|(sequence, cid)| sequence.get() >= 5 && cid == active),
        "the peer eventually selects a CID stored in a generation-reused slot",
    );

    let final_at = cycle_at + Duration::from_millis(100);
    assert!(
        Count::send(&mut client, outbound, vec![0x30], final_at).is_ok(),
        "live client connection must accept the final datagram"
    );
    let deadline = (Pin::new(&mut client).observe(final_at, |turn| turn.deadline()))
        .expect("application datagram owns a mux deadline");
    let mut send_at = deadline.max(final_at);
    let mut recovery_bound = None;
    loop {
        assert_eq!(
            client
                .conn(outbound)
                .expect("live client")
                .inspection()
                .tx_queued(),
            1,
            "the final DATAGRAM remains owned until packet construction"
        );
        let wires = Count::relay_at(&mut client, &mut server, local, send_at)
            .expect("final DATAGRAM and control output");
        assert!(
            wires
                .iter()
                .filter_map(|wire| Count::short_cid(wire))
                .all(|cid| cid == active)
        );
        let acknowledgments = Count::relay_at(&mut server, &mut client, peer, send_at)
            .expect("deliver final DATAGRAM and return acknowledgments");
        let conn = client.conn(outbound).expect("live client");
        if conn.inspection().tx_queued() == 0 {
            assert!(
                wires
                    .iter()
                    .filter_map(|wire| Count::short_cid(wire))
                    .any(|cid| cid == active),
                "the flight consuming the DATAGRAM uses the generation-reused CID"
            );
            break;
        }
        assert_eq!(server.handler().datagrams, 1);
        let bound = *recovery_bound.get_or_insert_with(|| {
            conn.view()
                .recovery()
                .next_timer()
                .expect("pending output retains a recovery deadline")
        });
        let next = [
            Pin::new(&mut client).observe(send_at, |turn| turn.deadline()),
            Pin::new(&mut server).observe(send_at, |turn| turn.deadline()),
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("the queued DATAGRAM or its acknowledgment owns a wake");
        assert!(
            next <= bound && (next > send_at || (next == send_at && !acknowledgments.is_empty())),
            "delivery progresses within its recovery deadline"
        );
        send_at = next;
    }
    assert_eq!(server.handler().datagrams, 2);
}
