use std::net::Shutdown;

use dope::{
    core::driver::{
        life::recovery,
        route::{Id, table::conn::Capacity},
    },
    manifold::{app::Host, timing::Balanced},
    net::{link::event::SendFailure, tcp::Tcp, wire::Identity},
    runtime::executor::{drive, session::Completion},
};
use dope_fiber::{
    BlockOn as _,
    net::{
        client::Plan,
        error::{
            Write,
            read::{Read, Transmit},
        },
    },
};
use dope_test::{
    harness::{
        peer::{Peer, TcpAcceptor},
        scenario,
    },
    reclaim::Reports,
};

pub mod common;

use common::{
    recover,
    runtime::{self, Setup},
};

#[test]
fn reports_send_failure() {
    let listener = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("listener");
    let addr = listener.local_addr().expect("listener address");
    let plan = Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity);
    let setup = Setup::try_for_plan::<Balanced, _>(&plan).expect("driver configuration");
    let mut reports = Reports::new(setup.config(), [Id::fixed::<0>()]);
    setup.with_input(
        plan,
        |mut sess| {
            let mut finish = Completion::new();
            let store = sess
                .storage()
                .try_build(sess.driver().storage)
                .expect("port");
            let result = store.machine(
                &mut sess,
                |sess| sess.driver(),
                |link, sess| {
                    let handle = store.handle();
                    sess.with_quiesce(
                        &mut finish,
                        Host::new(link),
                        scenario::TIMEOUT,
                        |mut app| {
                            let mut stream =
                                app.block_on(handle.connect(addr))?.ok().expect("connect");
                            let peer = listener
                                .accept_for(scenario::TIMEOUT)
                                .expect("accept peer")
                                .expect("connected peer");
                            peer.shutdown(Shutdown::Write).expect("receive EOF");
                            assert!(matches!(app.block_on(stream.read())?, Ok(None)));
                            Peer::try_reset(&peer).expect("reset on close");
                            drop(peer);
                            assert!(matches!(
                                app.block_on(stream.write_all(b"x"))?,
                                Err(Write::ConnectionFailed { payload: None })
                            ));
                            assert!(matches!(app.block_on(stream.read())?,
                                Err(Read::Transmit(Transmit::Io(SendFailure::Queued(error))))
                                    if error.is_pipe() || error.is_reset()
                            ));
                            assert!(matches!(app.block_on(stream.read())?, Ok(None)));
                            Ok::<_, drive::Error>(())
                        },
                    )
                    .expect("client lifecycle")
                    .expect("client progress");
                },
            );
            assert!(result.is_ok(), "client installation");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        runtime::check_scope,
    );
    assert!(
        reports.events().expect("native reclamation").is_empty(),
        "unexpected reclamation: {reports:?}"
    );
}
