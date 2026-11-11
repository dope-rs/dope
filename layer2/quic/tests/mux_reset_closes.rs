pub mod common;

use common::{
    peer,
    runtime::{
        Error,
        reap::{CLIENT, Capture, SERVER},
    },
};
use dope_quic::{
    conn::life::{accept::Authority, options::Options},
    proto::param,
    runtime::mux::{builder::Builder, manage},
};
use std::{net::SocketAddr, pin::Pin, time::Instant};

#[test]
fn reset_closes_match() -> Result<(), Error> {
    for (first, collide) in [
        (0x40, false),
        (0x00, false),
        (0xC0, false),
        (0x80, false),
        (0xE0, false),
        (0x40, true),
    ] {
        let wrong: SocketAddr = "10.0.0.3:443".parse().expect("unrelated test address");
        let (signing, key) = peer::raw(0x39).expect("raw identity");
        let source = signing.server().expect("server TLS");
        let tp = param::Set::builder()
            .idle_ms(30_000)
            .datagram(Some(65_535))
            .try_build();
        let tp = tp.expect("reap parameters");
        let server_h = Capture::default();
        let srv_opts = Options::builder()
            .transport(|net| net.params(tp.clone()))
            .security(|security| security.reset(Some([0xA5; 32])))
            .try_build();
        let srv_opts = (srv_opts).expect("fixture server options must be valid");
        let auth = Authority::new(source, srv_opts)
            .expect("test certificate source and options must form an authority");
        let mut server = Builder::server(server_h, auth)
            .build()
            .expect("test server router plan must be valid");
        let client_h = Capture::default();
        let cli_log = client_h.events.clone();
        let mut client = Builder::client(client_h)
            .build()
            .expect("test client router plan must be valid");
        let now = Instant::now();
        let handle = Capture::handshake(
            &mut server,
            &mut client,
            key,
            tp.clone(),
            SERVER,
            CLIENT,
            now,
        )?;
        let other = if collide {
            let other = Capture::connect(
                &mut client,
                SERVER,
                key,
                Options::try_from(tp.clone()).expect("second connection options"),
                vec![0xb6; 8],
                now,
            )?;
            Capture::relay_once(&mut client, &mut server, CLIENT, now)?;
            Capture::relay_once(&mut server, &mut client, SERVER, now)?;
            Capture::relay_once(&mut client, &mut server, CLIENT, now)?;
            Capture::drive(&mut client, now).map_err(Error::Output)?;
            Some(other)
        } else {
            None
        };
        let token = client
            .conn(handle)
            .and_then(|conn| conn.view().peer().params())
            .and_then(|params| params.identity().reset())
            .expect("server stateless reset token");
        let mut reset = vec![0; 30];
        reset[0] = first;
        reset[1..9].copy_from_slice(&[0xA7; 8]);
        let tail = reset.len() - 16;
        reset[tail..].copy_from_slice(&token);

        if let Some(other) = other {
            let cid = client
                .conn(other)
                .expect("second connection")
                .view()
                .path()
                .local_cids()
                .values()
                .next()
                .expect("second local CID");
            reset[1..1 + cid.len()].copy_from_slice(cid);
        } else {
            assert!(
                Capture::receive(&mut client, wrong, &mut reset, now).is_ok(),
                "client must process unrelated reset"
            );
            assert!(
                client
                    .conn(handle)
                    .is_some_and(|conn| conn.view().lifecycle().is_ready()),
                "a token received from an unrelated address must be ignored",
            );
        }

        assert!(
            Capture::receive(&mut client, SERVER, &mut reset, now).is_ok(),
            "client must process matching reset"
        );
        assert!(
            client
                .conn(handle)
                .is_some_and(|conn| conn.view().path().is_reset()),
            "the mux must route an unknown-DCID reset to the matching connection",
        );
        assert!(client.conn(handle).is_some_and(
            |conn| conn.view().lifecycle().state() == dope_quic::conn::State::Draining
        ));

        if let Some(other) = other {
            let conn = client.conn(other).expect("unrelated live connection");
            assert!(conn.view().lifecycle().is_ready());
            assert!(!conn.view().path().is_reset());
            let at = conn
                .view()
                .recovery()
                .send_at(now + std::time::Duration::from_millis(1));
            assert!(Capture::send(&mut client, other, vec![7], at).is_ok());
            Capture::relay_once(&mut client, &mut server, CLIENT, at)?;
            Capture::drive(&mut server, at).map_err(Error::Output)?;
            assert!(
                server
                    .handler()
                    .events
                    .borrow()
                    .datagrams
                    .iter()
                    .any(|(_, data)| data == &[7])
            );
            assert_eq!(cli_log.borrow().closed, [handle]);
            continue;
        }

        Capture::drive(&mut client, now).map_err(Error::Output)?;
        assert!(client.conn(handle).is_some());
        assert!(cli_log.borrow().closed == vec![handle]);
        let deadline = Pin::new(&mut client)
            .observe(now, |turn| turn.deadline())
            .expect("reset drain deadline");
        assert!(
            Capture::output(&mut client, now)
                .map_err(Error::Output)?
                .is_empty()
        );
        Capture::receive(&mut client, SERVER, &mut reset, now).expect("duplicate reset");
        assert!(
            Capture::output(&mut client, now)
                .map_err(Error::Output)?
                .is_empty()
        );
        assert_eq!(
            Pin::new(&mut client).observe(now, |turn| turn.deadline()),
            Some(deadline)
        );
        Capture::drive(&mut client, deadline).map_err(Error::Output)?;
        assert!(client.conn(handle).is_none());

        let remote = server.sole().expect("single server connection");
        Capture::close(&mut server, remote, now).expect("close the first server connection");
        Capture::drive(&mut server, now).map_err(Error::Output)?;
        let retired = Pin::new(&mut server)
            .observe(now, |turn| turn.deadline())
            .expect("server close deadline");
        Capture::drive(&mut server, retired).map_err(Error::Output)?;
        let retained = Capture::handshake(&mut server, &mut client, key, tp, SERVER, CLIENT, now)?;
        let conn = client.conn(retained).expect("reconnected client");
        assert!(conn.view().lifecycle().is_ready());
        assert!(
            conn.view()
                .peer()
                .params()
                .and_then(|params| params.identity().reset())
                .is_some()
        );
        assert!((0..=u16::MAX).any(|_| {
            Pin::new(&mut client).observe(now, |mut turn| turn.lifecycle().advance())
                == manage::Status::Complete
        }));
        assert!(client.conn(retained).is_none());
        assert!(cli_log.borrow().closed == vec![handle, retained]);
        assert!(!client.handler().events.borrow().release_after_close);
        assert!(!server.handler().events.borrow().release_after_close);
    }
    Ok(())
}

#[test]
fn server_recognizes_initial_and_version_reset_forms() {
    use common::suite::{
        mux::{dispatch, paths},
        peers::packet::bytes,
    };
    use dope_quic::wire::frame;

    for form in 0..4 {
        let (mut peer, mut server, handle, now) = paths::try_wire().expect("wire peer");
        let mut notice = peer
            .packet(&bytes::notice(1, 1).expect("CID notice"))
            .expect("notice packet");
        paths::Pair::receive(&mut server, paths::CLIENT, &mut notice, now)
            .expect("replacement CID");
        let output = dispatch::Capture::output(&mut server, now).expect("use replacement CID");
        assert!(!output.is_empty());
        for packet in output {
            Pin::new(&mut server).output().recycle(packet);
        }
        let mut wire = if form == 3 {
            let cid = server
                .conn(handle)
                .expect("live server")
                .view()
                .path()
                .local_cids()
                .values()
                .next()
                .expect("server CID")
                .to_vec();
            let mut wire = vec![0x80, 0, 0, 0, 0, cid.len() as u8];
            wire.extend_from_slice(&cid);
            wire.push(0);
            wire.extend_from_slice(&[1; 16]);
            wire
        } else {
            let mut wire = peer::Initial::new(&[0xd7; 8], &[0xd8; 8], 0)
                .seal(&[frame::TYPE_PING])
                .expect("Initial wire");
            if form == 0 {
                wire.truncate(30);
            }
            if form == 2 {
                wire[1..5].copy_from_slice(&0xabcd1234_u32.to_be_bytes());
            }
            *wire.last_chunk_mut::<16>().expect("token tail") = [1; 16];
            wire
        };
        paths::Pair::receive(&mut server, paths::CLIENT, &mut wire, now)
            .expect("matching reset form");
        let conn = server.conn(handle).expect("draining server");
        assert!(conn.view().path().is_reset(), "form {form}");
        assert_eq!(
            conn.view().lifecycle().state(),
            dope_quic::conn::State::Draining
        );
        assert!(
            dispatch::Capture::output(&mut server, now)
                .expect("reset output")
                .is_empty()
        );
    }
}
