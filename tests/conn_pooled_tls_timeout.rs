use dope_quic::conn::life::options::Options;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, Instant};

use dope_quic::conn::life::accept;
use dope_quic::errors::Connect;
use dope_quic::proto::param;
use dope_quic::runtime::mux::builder;
use dope_quic::runtime::routing::Plain;

pub mod common;
use common::suite::peers::tls::Capture;

#[test]
fn rejects_initial_before_tls_admission() {
    use common::suite::mux::install;
    use dope_quic::{conn, errors, proto::epoch, security::crypto};

    let (servers, _, cid) =
        Capture::prepare(0x5b, false, Options::default()).expect("pooled TLS configuration");
    let mut server = builder::Builder::server_pool(Capture::default(), &servers)
        .limits(builder::Plan::try_new(1, 32, 1 << 20).expect("mux limits"))
        .build()
        .expect("server mux");
    let from = SocketAddr::from(([10, 1, 0, 1], 50_000));
    let now = Instant::now();
    let dcid = [0x51; 8];
    let scid = [0x52; 8];
    let mut invalid = install::initial(&dcid, &scid).expect("Initial");
    *invalid.last_mut().expect("AEAD tag") ^= 1;
    let result = Pin::new(&mut server).observe(now, |mut turn| {
        turn.protocol().recv(from, &mut invalid, epoch::WALL_CLOCK)
    });
    assert!(matches!(
        result,
        Err(errors::Receive::Packet(conn::Error::Crypto(
            crypto::Error::Decrypt(rustls::Error::DecryptError)
        )))
    ));
    assert_eq!(server.active_conns(), 0);
    assert!(server.handler().last_created.is_none());
    drop(
        servers
            .accept(accept::Ids::initial(cid, cid, cid))
            .expect("unused TLS permit"),
    );

    let mut initial = install::initial(&dcid, &scid).expect("Initial");
    let mut invalid = initial.clone();
    *invalid.last_mut().expect("AEAD tag") ^= 1;
    let mut coalesced = invalid.clone();
    coalesced.extend_from_slice(&initial);
    let result = Pin::new(&mut server).observe(now, |mut turn| {
        turn.protocol()
            .recv(from, &mut coalesced, epoch::WALL_CLOCK)
    });
    assert!(matches!(
        result,
        Err(errors::Receive::Packet(conn::Error::Crypto(
            crypto::Error::Decrypt(rustls::Error::DecryptError)
        )))
    ));
    assert_eq!(server.active_conns(), 1, "authenticated suffix admits once");
    initial.extend_from_slice(&invalid);
    let result = Pin::new(&mut server).observe(now, |mut turn| {
        turn.protocol().recv(from, &mut initial, epoch::WALL_CLOCK)
    });
    assert!(matches!(
        result,
        Err(errors::Receive::Packet(conn::Error::Crypto(
            crypto::Error::Decrypt(rustls::Error::DecryptError)
        )))
    ));
    assert_eq!(server.active_conns(), 1);
    let handle = server
        .handler()
        .last_created
        .expect("authenticated Initial");
    assert!(matches!(
        servers.accept(accept::Ids::initial(cid, cid, cid)),
        Err(Connect::Capacity)
    ));
    let result = Pin::new(&mut server).observe(now, |mut turn| {
        turn.protocol().recv(from, &mut invalid, epoch::WALL_CLOCK)
    });
    assert!(matches!(
        result,
        Err(errors::Receive::Packet(conn::Error::Crypto(
            crypto::Error::Decrypt(rustls::Error::DecryptError)
        )))
    ));
    assert!(
        server
            .conn(handle)
            .is_some_and(|conn| conn.view().lifecycle().is_opening())
    );
}

#[test]
fn returns_timeout_leases() {
    let short = || {
        let params = param::Set::builder()
            .idle_ms(1)
            .try_build()
            .expect("fixture transport parameters");
        Options::try_from(params).expect("fixture configuration must satisfy its declared bounds")
    };
    let (servers, clients, cid) =
        Capture::prepare(0x5a, false, short()).expect("pooled TLS configuration");
    let limits = (builder::Plan::try_new(2, 32, 1 << 20)).expect("pooled mux limits");
    let server = builder::Builder::server_pool(Capture::default(), &servers);
    let mut server = server
        .limits(limits)
        .build()
        .expect("pooled server mux allocation");
    let mut client = (builder::Builder::client_pool(Capture::default())
        .limits(limits)
        .build())
    .expect("pooled client mux allocation");
    let remote = SocketAddr::from(([10, 1, 0, 2], 443));
    let local = SocketAddr::from(([10, 1, 0, 1], 50_000));
    let started = Instant::now();
    let handle = (Pin::new(&mut client).observe(started, |mut turn| {
        turn.protocol()
            .pool_connect(remote, &clients, short(), vec![0x51; 8])
    }))
    .expect("timed client connection");
    Capture::relay(&mut client, &mut server, local, started).expect("pooled TLS relay");
    let peer = (server.handler().last_created).expect("server accepted the timed connection");
    assert!(matches!(
        clients.connect::<0>(
            cid,
            cid,
            Capture::options().expect("fixture transport options"),
            Plain,
        ),
        Err(Connect::Capacity)
    ));
    assert!(matches!(
        servers.accept(accept::Ids::initial(cid, cid, cid)),
        Err(Connect::Capacity)
    ));
    Capture::discard(&mut server, started).expect("pooled TLS discard");
    let conn = (client.conn(handle)).expect("timed client connection remains live");
    let local_at =
        (conn.view().recovery().next_timer()).expect("timed client connection has a deadline");
    let conn = (server.conn(peer)).expect("timed server connection remains live");
    let peer_at =
        (conn.view().recovery().next_timer()).expect("timed server connection has a deadline");
    let expired = local_at.max(peer_at) + Duration::from_secs(3);
    Pin::new(&mut client).observe(expired, |mut turn| {
        assert!(
            turn.output().drive().is_ok(),
            "expired client closes without admission failure"
        );
    });
    Pin::new(&mut server).observe(expired, |mut turn| {
        assert!(
            turn.output().drive().is_ok(),
            "expired server closes without admission failure"
        );
    });
    assert!(client.conn(handle).is_none());
    assert!(server.conn(peer).is_none());
    assert_eq!(client.active_conns(), 0);
    assert_eq!(server.active_conns(), 0);
    let client2 = clients
        .connect::<0>(
            cid,
            cid,
            Capture::options().expect("fixture transport options"),
            Plain,
        )
        .expect("client timeout returns its lease");
    let server2 = (servers.accept(accept::Ids::initial(cid, cid, cid)))
        .expect("server timeout returns its lease");
    assert!(client2.view().lifecycle().is_opening());
    assert!(server2.view().lifecycle().is_opening());
}
