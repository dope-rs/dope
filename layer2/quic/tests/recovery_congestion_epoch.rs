pub mod common;

use common::peer::{Pair, Receiver as _};
use common::suite::peers::receive::datagram;
use dope_quic::conn::{Epoch, wire::packet::Batch};
use std::time::{Duration, Instant};

#[test]
fn persistent_losses_span_ack_batches() {
    use common::{peer::Scope as _, suite::peers::packet};
    use dope_quic::wire::{frame, varint::VarInt};

    for (acknowledged_gap, untracked_gap) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let clock = Instant::now();
        let mut pair = packet::Pair::try_new(1, clock, |_| {}).expect("independent peer");
        pair.retired(clock).expect("settled handshake");
        assert_eq!(pair.server.view().recovery().unacked(Epoch::Application), 0);
        assert!(pair.server.view().recovery().smoothed_rtt().is_some());
        let stream = pair.server.send_streams().open_uni().expect("stream");
        let base = clock + Duration::from_secs(1);
        let later = base + Duration::from_secs(10);
        let mut batch = Batch::default();
        let mut previous = None;
        let mut old_ack = Vec::new();

        for (index, sent) in [
            base,
            base + Duration::from_secs(5),
            later,
            later + Duration::from_micros(10),
            later + Duration::from_millis(2),
        ]
        .into_iter()
        .enumerate()
        {
            if index == 1 && (untracked_gap || !acknowledged_gap) {
                if !untracked_gap {
                    continue;
                }
                pair.receive(&[frame::TYPE_PING], sent)
                    .expect("ACK stimulus");
                let tracked = pair.server.view().recovery().unacked(Epoch::Application);
                pair.server
                    .with_scope(|mut scope| {
                        scope.output().send_batch(
                            &mut batch,
                            sent + Duration::from_millis(25),
                            1,
                            1_200,
                        )
                    })
                    .expect("pure ACK between lost packets");
                assert_eq!(batch.packets(), 1);
                let (number, body) = pair
                    .peer
                    .open(batch.iter_mut().next().expect("one ACK packet"))
                    .expect("authenticated pure ACK");
                assert!(
                    frame::Frame::decode_all(&body)
                        .expect("ACK frames")
                        .iter()
                        .all(|frame| matches!(
                            frame.view(),
                            frame::View::Ack { .. } | frame::View::Padding
                        ))
                );
                assert_eq!(
                    pair.server.view().recovery().unacked(Epoch::Application),
                    tracked
                );
                assert_eq!(number, previous.expect("first lost packet") + 1);
                previous = Some(number);
                if acknowledged_gap {
                    let mut ack = Vec::new();
                    frame::Borrowed::ack(
                        VarInt::try_from(number).expect("pure ACK packet number"),
                        VarInt::ZERO,
                        VarInt::ZERO,
                        &[],
                    )
                    .expect("receipt for the pure ACK")
                    .encode(&mut ack)
                    .expect("encoded receipt");
                    pair.receive(&ack, sent + Duration::from_millis(26))
                        .expect("acknowledged untracked gap");
                }
                continue;
            }
            pair.server
                .send_streams()
                .send(stream, b"x")
                .expect("payload");
            pair.server
                .with_scope(|mut scope| scope.output().send_batch(&mut batch, sent, 1, 1_200))
                .expect("one tracked packet");
            assert_eq!(batch.packets(), 1);
            let (number, _) = pair
                .peer
                .open(batch.iter_mut().next().expect("one packet"))
                .expect("authenticated packet");
            if let Some(prior) = previous {
                assert_eq!(number, prior + 1);
            }
            previous = Some(number);
            if index == 0 {
                frame::Borrowed::ack(
                    VarInt::try_from(number.checked_sub(1).expect("earlier handshake packet"))
                        .expect("old packet number"),
                    VarInt::ZERO,
                    VarInt::ZERO,
                    &[],
                )
                .expect("old ACK")
                .encode(&mut old_ack)
                .expect("encoded old ACK");
            }
            let received = match index {
                1 => sent + Duration::from_micros(100),
                3 => later + Duration::from_micros(100),
                4 => later + Duration::from_millis(3),
                _ => continue,
            };
            let mut ack = Vec::new();
            frame::Borrowed::ack(
                VarInt::try_from(number).expect("packet number"),
                VarInt::ZERO,
                VarInt::ZERO,
                &[],
            )
            .expect("selective ACK")
            .encode(&mut ack)
            .expect("encoded ACK");
            pair.receive(&ack, received).expect("loss feedback");
            if index == 3 {
                pair.receive(&ack, received + Duration::from_nanos(1))
                    .expect("duplicate later ACK");
                pair.receive(&old_ack, received + Duration::from_nanos(2))
                    .expect("old ACK before the current loss interval");
            }
            assert_eq!(
                pair.server.view().recovery().unacked(Epoch::Application),
                if index == 3 { 1 } else { 0 },
            );
            if index != 4 {
                assert!(
                    pair.server.view().recovery().window() > pair.server.view().path().mtu() * 2
                );
            }
        }
        let minimum = pair.server.view().path().mtu() * 2;
        let window = pair.server.view().recovery().window();
        if acknowledged_gap {
            assert!(
                window > minimum,
                "an ACK inside the loss interval breaks persistence"
            );
        } else {
            assert_eq!(
                window, minimum,
                "later ACKs preserve earlier consecutive losses"
            );
        }
    }
}

#[test]
fn higher_ack_releases_pto_time_loss() {
    use common::{peer::Scope as _, suite::peers::packet};
    use dope_quic::wire::{frame, varint::VarInt};

    let clock = Instant::now();
    let mut pair = packet::Pair::try_new(1, clock, |_| {}).expect("independent peer");
    pair.retired(clock).expect("settled handshake");
    assert_eq!(pair.server.view().recovery().unacked(Epoch::Application), 0);
    assert!(pair.server.view().recovery().smoothed_rtt().is_some());
    let stream = pair.server.send_streams().open_uni().expect("stream");
    pair.server
        .send_streams()
        .send(stream, b"x")
        .expect("lost payload");
    let base = clock + Duration::from_secs(1);
    let later = base + Duration::from_secs(10);
    let mut batch = Batch::default();
    let mut previous = None;
    for (index, sent) in [base, later, later + Duration::from_millis(200)]
        .into_iter()
        .enumerate()
    {
        pair.server
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, sent, 1, 1_200))
            .expect("one tracked carrier");
        assert_eq!(batch.packets(), 1);
        let (number, _) = pair
            .peer
            .open(batch.iter_mut().next().expect("one packet"))
            .expect("authenticated carrier");
        if let Some(previous) = previous {
            assert_eq!(number, previous + 1);
        }
        previous = Some(number);
        if index == 0 {
            let window = pair.server.view().recovery().window();
            let due = pair.server.view().recovery().next_timer().expect("PTO");
            pair.server
                .with_scope(|mut scope| scope.recovery().try_loss(due))
                .expect("PTO arms probes without declaring loss");
            assert_eq!(pair.server.view().recovery().unacked(Epoch::Application), 1);
            assert_eq!(pair.server.view().recovery().window(), window);
        }
    }
    let mut ack = Vec::new();
    frame::Borrowed::ack(
        VarInt::try_from(previous.expect("later carrier")).expect("packet number"),
        VarInt::ZERO,
        VarInt::ZERO,
        &[],
    )
    .expect("later carrier ACK")
    .encode(&mut ack)
    .expect("encoded ACK");
    pair.receive(&ack, later + Duration::from_micros(200_100))
        .expect("ACK permits time-threshold loss of earlier carriers");
    assert_eq!(pair.server.view().recovery().unacked(Epoch::Application), 0);
    assert_eq!(
        pair.server.view().recovery().window(),
        pair.server.view().path().mtu() * 2
    );
}

#[test]
fn old_flight_reduces_once() {
    let mut pair = Pair::try_default().expect("established peers");
    let base = datagram::try_settle(&mut pair, Instant::now() + Duration::from_secs(1))
        .expect("settled acknowledgments")
        + Duration::from_secs(1);
    let Pair {
        mut client,
        mut server,
        mut work,
    } = pair;
    assert_eq!(client.view().recovery().unacked(Epoch::Application), 0);
    let original = client.view().recovery().window();
    let mut packets = Vec::new();
    let mut batch = Batch::default();
    for sequence in 0..5 {
        client
            .with_scope(|mut scope| scope.datagrams().try_send(vec![0x41; 128]))
            .expect("old flight datagram");
        let at = base + Duration::from_millis(sequence);
        client
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, at, 1, 1_200))
            .expect("old flight packet");
        assert_eq!(batch.packets(), 1);
        packets.push(batch.iter_mut().next().expect("one packet").to_vec());
    }

    server
        .receive(&mut work, &mut packets[3], base + Duration::from_millis(90))
        .expect("first acknowledged packet");
    client
        .receive_from(&mut work, &mut server, base + Duration::from_millis(100))
        .expect("first congestion event");
    let reduced = client.view().recovery().window();
    assert!(reduced < original);
    assert_eq!(client.view().recovery().unacked(Epoch::Application), 3);

    server
        .receive(
            &mut work,
            &mut packets[4],
            base + Duration::from_millis(101),
        )
        .expect("second acknowledged packet");
    client
        .receive_from(&mut work, &mut server, base + Duration::from_millis(126))
        .expect("later loss from the old flight");
    assert_eq!(client.view().recovery().unacked(Epoch::Application), 1);
    assert_eq!(client.view().recovery().window(), reduced);

    for sequence in 0..4 {
        client
            .with_scope(|mut scope| scope.datagrams().try_send(vec![0x42; 128]))
            .expect("new flight datagram");
        let at = base + Duration::from_millis(200 + sequence);
        client
            .with_scope(|mut scope| scope.output().send_batch(&mut batch, at, 1, 1_200))
            .expect("new flight packet");
        assert_eq!(batch.packets(), 1);
    }
    server
        .receive_all(&mut work, &mut batch, base + Duration::from_millis(290))
        .expect("new flight's final packet");
    client
        .receive_from(&mut work, &mut server, base + Duration::from_millis(300))
        .expect("new congestion event");
    assert_eq!(
        client.view().recovery().window(),
        (reduced / 2).max(client.view().path().mtu() * 2),
        "a loss transmitted after recovery began starts a new recovery period",
    );
}
