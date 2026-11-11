pub mod common;
use crate::common::peer;
use std::time::{Duration, Instant};

use dope_quic::wire::packet::cid;
use dope_quic::{conn, security::crypto};
use peer::Receiver as _;
use peer::{Config, Pair, sealer};
use rustls::crypto::aws_lc_rs::cipher_suite;

#[test]
fn retains_reordered_epoch() {
    for suite in [
        cipher_suite::TLS13_AES_128_GCM_SHA256,
        cipher_suite::TLS13_AES_256_GCM_SHA384,
        cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
    ] {
        let mut pair = Pair::try_config(
            Config::default().try_build().expect("server transport"),
            Config::default().try_build().expect("client transport"),
            cid::Id::try_from([0x71; 8]).expect("fixture connection ID"),
            |config, _| sealer::suite(config, suite).expect("native QUIC cipher suite"),
        )
        .expect("the peers must open");
        let mut now = Instant::now();
        pair.exchange(now, 6).expect("the peers must establish");
        let Pair {
            mut server,
            mut client,
            mut work,
        } = pair;
        assert!(client.view().lifecycle().is_confirmed(), "{suite:?}");
        assert!(server.view().lifecycle().is_confirmed(), "{suite:?}");

        let [mut peer_old, mut peer_grace, mut peer_expired] = [
            b"peer old phase".as_slice(),
            b"peer grace phase",
            b"peer expired phase",
        ]
        .map(|payload| {
            server
                .with_scope(|mut scope| scope.datagrams().try_send(payload.to_vec()))
                .expect("the peer's old-phase datagram must be admitted");
            now = server.view().recovery().send_at(now);
            let (packets, send_result) = server.with_scope(|mut scope| scope.output().send(now));
            send_result.expect("the peer's old-phase datagram must enter a packet");
            assert!(!packets.is_empty());
            packets
        });

        assert!(
            client
                .with_scope(|mut scope| scope.datagrams().try_send(b"old phase".to_vec()))
                .is_ok()
        );
        now = client.view().recovery().send_at(now);
        let (mut old, send_result) = client.with_scope(|mut scope| scope.output().send(now));
        send_result.expect("the old-phase datagram must enter a packet");
        assert_eq!(client.inspection().tx_queued(), 0);
        assert!(
            client
                .with_scope(|mut scope| scope.security().at(now).update())
                .is_ok()
        );
        assert!(matches!(
            client.with_scope(|mut scope| scope.security().at(now).update()),
            Err(conn::Error::KeyUpdate),
        ));

        now += Duration::from_secs(5);
        for packet in &mut peer_old {
            client
                .receive(&mut work, packet, now)
                .expect("old keys must survive until a new-phase packet authenticates");
        }
        assert_eq!(
            client
                .with_scope(|mut scope| scope.datagrams().recv())
                .as_deref(),
            Some(b"peer old phase".as_slice()),
        );

        assert!(
            client
                .with_scope(|mut scope| scope.datagrams().try_send(b"new phase".to_vec()))
                .is_ok()
        );
        now = client.view().recovery().send_at(now);
        assert!(server.receive_from(&mut work, &mut client, now).is_ok());
        assert_eq!(
            server
                .with_scope(|mut scope| scope.datagrams().recv())
                .as_deref(),
            Some(b"new phase".as_slice()),
        );
        for packet in &mut old {
            assert!(server.receive(&mut work, packet, now).is_ok());
        }
        assert_eq!(
            server
                .with_scope(|mut scope| scope.datagrams().recv())
                .as_deref(),
            Some(b"old phase".as_slice()),
        );

        assert!(
            server
                .with_scope(|mut scope| scope.datagrams().try_send(b"response phase".to_vec()))
                .is_ok()
        );
        now = server.view().recovery().send_at(now);
        assert!(client.receive_from(&mut work, &mut server, now).is_ok());
        assert_eq!(
            client
                .with_scope(|mut scope| scope.datagrams().recv())
                .as_deref(),
            Some(b"response phase".as_slice()),
        );

        now += Duration::from_millis(1);
        for packet in &mut peer_grace {
            client
                .receive(&mut work, packet, now)
                .expect("old-key grace must start at new-phase authentication");
        }
        assert_eq!(
            client
                .with_scope(|mut scope| scope.datagrams().recv())
                .as_deref(),
            Some(b"peer grace phase".as_slice()),
        );
        now += Duration::from_secs(5);
        for packet in &mut peer_expired {
            assert!(matches!(
                client.receive(&mut work, packet, now),
                Err(peer::Transfer::Input(conn::Error::Crypto(
                    crypto::Error::Decrypt(rustls::Error::DecryptError)
                )))
            ));
        }

        let next = client
            .view()
            .recovery()
            .send_at(now + Duration::from_secs(10));
        assert!(
            client
                .with_scope(|mut scope| scope.security().at(next).update())
                .is_ok()
        );
        assert!(
            client
                .with_scope(|mut scope| scope.datagrams().try_send(b"second update".to_vec()))
                .is_ok()
        );
        assert!(server.receive_from(&mut work, &mut client, next).is_ok());
        assert_eq!(
            server
                .with_scope(|mut scope| scope.datagrams().recv())
                .as_deref(),
            Some(b"second update".as_slice()),
        );
    }
}

#[test]
fn rejects_old_keys_above_reordered_current() {
    use common::suite::peers::packet;
    use dope_quic::wire::frame;

    let now = Instant::now();
    let options = Config::default().try_build().expect("datagram transport");
    let mut pair = packet::Pair::try_options(options, now, |_| {}).expect("native handshake");
    let old_first = pair
        .peer
        .packet(&[frame::TYPE_DATAGRAM, 0])
        .expect("old first");
    let old_lower = pair
        .peer
        .packet(&[frame::TYPE_DATAGRAM, 1])
        .expect("old lower");
    let old_key = pair.peer.update_key(None).expect("next native key");
    let new_lower = pair
        .peer
        .packet(&[frame::TYPE_DATAGRAM, 2])
        .expect("new lower");
    let new_key = pair
        .peer
        .update_key(Some(old_key))
        .expect("retained old key");
    let mut old_higher = pair
        .peer
        .packet(&[frame::TYPE_DATAGRAM, 3])
        .expect("old higher");
    drop(
        pair.peer
            .update_key(Some(new_key))
            .expect("restore current key"),
    );
    let new_higher = pair
        .peer
        .packet(&[frame::TYPE_DATAGRAM, 4])
        .expect("new higher");

    for (mut packet, value) in [
        (old_first, 0),
        (new_higher, 4),
        (new_lower, 2),
        (old_lower, 1),
    ] {
        pair.server
            .receive(&mut pair.work, &mut packet, now)
            .expect("valid packet reordering");
        assert_eq!(
            pair.server.with_scope(|mut scope| scope.datagrams().recv()),
            Some(vec![value])
        );
    }
    assert!(matches!(
        pair.server.receive(&mut pair.work, &mut old_higher, now),
        Err(peer::Transfer::Input(conn::Error::Crypto(
            crypto::Error::Decrypt(rustls::Error::DecryptError)
        )))
    ));
    assert!(
        pair.server
            .with_scope(|mut scope| scope.datagrams().recv())
            .is_none()
    );
    assert!(pair.server.view().lifecycle().is_ready());
}
