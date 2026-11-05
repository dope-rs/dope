pub mod common;

use common::suite::{
    mux::{install, retry},
    retry::Capture,
};
use dope_quic::proto::epoch;
use dope_quic::runtime::mux::builder::Plan;
use dope_quic::wire::packet::QUIC_V1;
use std::{net::SocketAddr, pin::Pin, time::Instant};

#[test]
fn server_negotiates() {
    let mut server = retry::try_server(Default::default(), Plan::standard()).expect("retry server");
    let peer: SocketAddr = "192.0.2.10:44321"
        .parse()
        .expect("test peer address must be valid");
    for (dcid_len, scid_len, first) in [(8, 12, 0xc0), (0, 255, 0xb0), (255, 0, 0x80)] {
        let dcid = vec![0x31; dcid_len];
        let scid = vec![0x42; scid_len];
        let mut request = install::unsupported(&dcid, &scid, 1200);
        request[0] = first;

        Pin::new(&mut server)
            .observe(Instant::now(), |mut turn| {
                turn.protocol().recv(peer, &mut request, epoch::WALL_CLOCK)
            })
            .expect("server must process the unsupported-version probe");

        assert_eq!(server.active_conns(), 0);
        let output = Pin::new(&mut server).observe(Instant::now(), |mut turn| {
            assert!(
                turn.output().drive().is_ok(),
                "version-negotiation mux connections must admit output"
            );
            turn.output().drain().collect::<Vec<_>>()
        });
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].addr(), peer);
        assert!(output[0].payload().len() <= request.len());
        let mut expected = install::negotiation(&scid, &dcid, &[QUIC_V1]);
        expected[0] = 0xc0;
        assert_eq!(output[0].payload(), expected);
    }
    assert_eq!(server.handler().certificates, 0);
}

#[test]
fn undersized_probe_is_discarded() {
    let mut server = retry::try_server(Default::default(), Plan::standard()).expect("retry server");
    let peer: SocketAddr = "192.0.2.11:44321"
        .parse()
        .expect("test peer address must be valid");
    let mut request = install::unsupported(&[1], &[2], 1199);

    Pin::new(&mut server)
        .observe(Instant::now(), |mut turn| {
            turn.protocol().recv(peer, &mut request, epoch::WALL_CLOCK)
        })
        .expect("server must discard the undersized unsupported-version probe");
    assert!(
        Capture::take(&mut server, Instant::now())
            .expect("retry take")
            .is_none()
    );
    assert_eq!(server.active_conns(), 0);
    assert_eq!(server.handler().certificates, 0);
}

#[test]
fn no_common_version_closes() {
    let (mut client, handle, server, local, orig) =
        install::attempt(0x74).expect("install attempt");
    let mut response = install::negotiation(&local, &orig, &[0xface_b00c]);

    Pin::new(&mut client)
        .observe(Instant::now(), |mut turn| {
            turn.protocol()
                .recv(server, &mut response, epoch::WALL_CLOCK)
        })
        .expect("client must process the CID-bound negotiation response");

    assert!(
        client
            .conn(handle)
            .expect("test connection handle must remain addressable")
            .view()
            .lifecycle()
            .is_closed()
    );
    assert_eq!(client.handler().certificates, 0);
}

#[test]
fn offered_version_ignored() {
    let (mut client, handle, server, local, orig) =
        install::attempt(0x75).expect("install attempt");
    let mut response = install::negotiation(&local, &orig, &[QUIC_V1, 0xface_b00c]);

    Pin::new(&mut client)
        .observe(Instant::now(), |mut turn| {
            turn.protocol()
                .recv(server, &mut response, epoch::WALL_CLOCK)
        })
        .expect("client must process the downgrade negotiation response");

    assert!(
        !client
            .conn(handle)
            .expect("test connection handle must remain addressable")
            .view()
            .lifecycle()
            .is_closed()
    );
    assert_eq!(client.handler().certificates, 0);
}
