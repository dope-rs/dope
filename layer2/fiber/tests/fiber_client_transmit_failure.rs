use std::{convert::Infallible, io, net::Shutdown};

use dope::{
    core::{
        driver::{life::recovery, route::table::conn::Capacity},
        platform::wake,
    },
    manifold::timing::Balanced,
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
use dope_test::harness::{peer::TcpScript, scenario};

pub mod common;

use common::{network::rounds::client::App, runtime::Setup};

#[test]
fn reports_send_failure() {
    let (reset, release) = wake::Ends::blocking().expect("reset gate").split();
    TcpScript::with(
        move |peer| {
            peer.shutdown(Shutdown::Write).expect("receive EOF");
            reset.wait_timeout(scenario::TIMEOUT).expect("EOF observed");
            peer.reset_on_close().expect("reset on close");
        },
        |peer| {
            let addr = peer.addr();
            let plan = Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity);
            let mut report = None;
            Setup::try_for_plan::<Balanced, _>(&plan)
                .expect("driver configuration")
                .with_input(
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
                                    App { connector: link },
                                    scenario::TIMEOUT,
                                    |mut app| {
                                        let mut stream = app
                                            .block_on(handle.connect(addr))?
                                            .ok()
                                            .expect("connect");
                                        assert!(matches!(app.block_on(stream.read())?, Ok(None)));
                                        release.notify().expect("release reset");
                                        peer.finish().expect("peer reset completed");
                                        assert!(matches!(
                                            app.block_on(stream.write_all(b"x"))?,
                                            Err(Write::ConnectionFailed { payload: None })
                                        ));
                                        assert!(matches!(app.block_on(stream.read())?,
                                            Err(Read::Transmit(Transmit::Io(SendFailure::Queued(error))))
                                                if matches!(io::Error::from(error).kind(),
                                                    io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset)
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
                    recovery::Policy::new(
                        |event| {
                            report.get_or_insert(event);
                        },
                        |failure: recovery::Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", failure.cause())
                        },
                    ),
                    |result| result.expect("driver lifecycle"),
                );
            assert!(report.is_none(), "unexpected reclamation: {report:?}");
        },
    )
    .expect("peer worker");
}
