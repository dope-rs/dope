use std::net::Shutdown;

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{Id, KeyTag, table::conn::Capacity},
        },
        io::socket::{self, Backlog},
    },
    manifold::{app::Host, network::listener::Domain, timing::Balanced},
    net::{
        link::event::SendFailure,
        tcp::Tcp,
        wire::{Identity, domain},
    },
    runtime::executor::session::Completion,
};
use dope_fiber::{
    BlockOn as _,
    net::{
        error::{
            Write,
            read::{Read, Transmit},
        },
        server::{Config, Listener, Plan},
    },
};
use dope_test::{
    harness::{GUARD, peer::Peer, scenario},
    reclaim::Reports,
};

pub mod common;

use common::{
    network::rounds::client::try_into_vec,
    recover,
    runtime::{self, Setup},
};

#[test]
fn writes_after_eof() {
    let held = socket::Listener::bind(([127, 0, 0, 1], 0).into()).expect("listener address");
    let addr = held.local_addr();
    let plan = Plan::<Identity>::new(Capacity::fixed::<1>(), Identity);
    let setup = Setup::try_for_plan::<Balanced, _>(&plan).expect("driver configuration");
    let mut reports = Reports::new(setup.config(), [Id::fixed::<0>()]);
    setup.with_input(
            plan,
            |mut sess| {
                let mut finish = Completion::new();
                let hash = sess.hash_state(Domain::DEFAULT);
                let storage = sess
                    .storage()
                    .try_build(sess.driver().storage)
                    .expect("listener port");
                let result = Listener::<KeyTag<0>, Tcp, Identity, _>::adopt(
                    &storage,
                    Config {
                        direct_flights: 0.into(),
                        bind: addr,
                        backlog: Backlog::fixed::<1>(),
                        stream: Default::default(),
                        transport: Default::default(),
                        egress: Default::default(),
                        bound: held,
                        wire: domain::Local::new(Identity),
                    },
                    |cause| cause.eliminate(),
                    hash,
                    &mut sess,
                    |sess| sess.driver(),
                    |listener, sess| {
                        let peer = Peer::at(addr).try_read(GUARD).expect("peer connection");
                        peer.write_wait(Some(GUARD)).expect("peer write timeout");
                        peer.set_nodelay(true).expect("peer no delay");
                        peer.write_all(b"tail").expect("peer tail");
                        peer.shutdown(Shutdown::Write).expect("peer EOF");
                        let handle = listener.handle();
                        sess.with_quiesce(
                                        &mut finish,
                                        Host::new(listener),
                                        scenario::TIMEOUT,
                                        |mut app| {
                                            app.block_on(dope_fiber::fiber!('_ => async move {
                                                let mut io = handle.accept().await.ok().expect("accept");
                                                let mut tail = Vec::new();
                                                while let Some(chunk) = io.read().await.ok().expect("receive") {
                                                    tail.extend_from_slice(&try_into_vec(chunk).expect("consume tail"));
                                                }
                                                assert_eq!(tail, b"tail");
                                                assert!(matches!(io.read().await, Ok(None)));
                                                io.write_all(b"o").await.ok().expect("first write after EOF");
                                                io.write_all(b"k").await.ok().expect("second write after EOF");
                                                let mut response = [0; 2];
                                                peer.read_exact(&mut response).expect("reply after EOF");
                                                assert_eq!(&response, b"ok");
                                                Peer::try_reset(&peer).expect("reset on close");
                                                drop(peer);
                                                assert!(matches!(io.write_all(b"x").await,
                                                    Err(Write::ConnectionFailed { payload: Some(payload) })
                                                        if payload.as_ref() == b"x"
                                                ));
                                                assert!(matches!(io.read().await,
                                                    Err(Read::Transmit(Transmit::Io(SendFailure::Queued(error))))
                                                        if error.is_pipe() || error.is_reset()
                                                ));
                                                assert!(matches!(io.read().await, Ok(None)));
                                            }))
                                        },
                                    )
                                    .expect("listener lifecycle")
                                    .expect("listener progress");
                    },
                );
                assert!(result.is_ok(), "listener installation");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            runtime::check_scope,
        );
    assert!(
        reports.events().expect("native reclamation").is_empty(),
        "unexpected reclamation: {reports:?}"
    );
}
