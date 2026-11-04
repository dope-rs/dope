pub mod common;

use common::{
    peer::Initial,
    suite::{
        mux::{install, retry},
        retry::Capture,
    },
};
use dope_quic::conn::life::options::Options;
use dope_quic::runtime::mux::builder::Plan;
use std::{net::SocketAddr, time::Instant};

#[test]
fn short_new_dcid_is_discarded() {
    let retry = Options::builder()
        .security(|security| security.retry([0xa1; 32]))
        .try_build()
        .expect("Retry policy");
    for options in [Options::default(), retry] {
        let mut mux = retry::try_server(options, Plan::standard()).expect("server");
        let from = SocketAddr::from(([127, 0, 0, 1], 55_001));
        let now = Instant::now();
        let mut packet = Initial::new(&[0x31; 7], &[0x42; 4], 0)
            .seal(&[0])
            .expect("authenticated Initial");
        Capture::receive(&mut mux, from, &mut packet, now).expect("discard short original DCID");
        assert_eq!(mux.active_conns(), 0);
        assert!(Capture::take(&mut mux, now).expect("output").is_none());
    }
}

#[test]
fn same_dcid_reuses_conn() {
    let mut mux = install::alias().expect("install alias");
    let from: SocketAddr = "127.0.0.1:55555"
        .parse()
        .expect("test peer address must be valid");
    let now = Instant::now();

    let dcid = [0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04];
    let scid = [0xaa, 0xbb, 0xcc, 0xdd];

    install::alias_admit(&mut mux, from, &dcid, &scid, now).expect("install alias_admit");
    assert_eq!(mux.active_conns(), 1, "first Initial opens one connection");

    install::alias_admit(&mut mux, from, &dcid, &scid, now).expect("install alias_admit");
    assert_eq!(
        mux.active_conns(),
        1,
        "second Initial with the same client DCID must reuse the connection"
    );
}

#[test]
fn dcids_open_conns() {
    let mut mux = install::alias().expect("install alias");
    let from: SocketAddr = "127.0.0.1:55556"
        .parse()
        .expect("test peer address must be valid");
    let now = Instant::now();

    install::alias_admit(&mut mux, from, &[1; 8], &[9, 9], now).expect("install alias_admit");
    install::alias_admit(&mut mux, from, &[2; 8], &[9, 9], now).expect("install alias_admit");
    assert_eq!(
        mux.active_conns(),
        2,
        "distinct client DCIDs are distinct connections"
    );
}
