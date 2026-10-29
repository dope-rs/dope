use dope::{core::driver::life::recovery, runtime::executor::session};
use dope_fiber::net::error::{Write, read::Read};
use dope_test::harness::{peer::TcpScript, scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{convert::Infallible, io::Read as _, net::Shutdown};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::tcp::Tcp,
    runtime::executor::drive,
};
use dope_fiber::{BlockOn as _, net::client::Plan};
use dope_test::reclaim::Reports;

use crate::common::network::rounds::client::fault::{Error, send::*};

#[test]
fn submits_before_error() {
    let mut finish = session::Completion::new();
    TcpScript::with(
        |stream| {
            stream
                .shutdown(Shutdown::Write)
                .expect("peer EOF must retain its receive direction");
            let mut req = [0u8; 1];
            stream
                .read_exact(&mut req)
                .expect("the send-failure peer must read the submitted byte");
            assert_eq!(&req, b"x");
        },
        |server| {
            let addr = server.addr();

            let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
            let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
            let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

            runtime.with_input(
                plan,
                |mut sess| {
                    let plan = sess.storage();
                    let store =
                        (plan.try_build(sess.driver().storage)).expect("client port storage");
                    let result = store.machine(
                        &mut sess,
                        |sess| sess.driver(),
                        |port, sess| {
                            let conn = store.handle();
                            let app = App { connector: port };
                            let driven = sess
                                .with_quiesce(
                                    &mut finish,
                                    app,
                                    scenario::TIMEOUT,
                                    |mut app| -> Result<(), drive::Error> {
                                        let opened = app.block_on(conn.connect(addr))?;
                                        let mut io = (opened)
                                            .ok()
                                            .expect("the send-failure connection must open");
                                        {
                                            let eof = app.block_on(io.read())?;
                                            assert!(matches!(eof, Ok(None)));
                                        }
                                        let write = app.block_on(io.write_all(b"x"))?;
                                        assert!(matches!(
                                            write,
                                            Err(Write::ConnectionFailed { payload: None })
                                        ));
                                        {
                                            let read = app.block_on(io.read())?;
                                            assert!(matches!(read,
                                                Err(Read::Wire(error))
                                                    if matches!(error.causes(), (Error::Protocol(
                                                        "domain-borrowed terminal failure"
                                                    ), None))
                                            ));
                                        }
                                        {
                                            let closed = app.block_on(io.read())?;
                                            assert!(
                                                matches!(closed, Ok(None)),
                                                "the send failure must be emitted only once"
                                            );
                                        }
                                        let first = app.block_on(io.write_all(b"first"))?;
                                        assert!(matches!(first,
                                            Err(Write::ConnectionClosed {
                                                payload: Some(payload)
                                            }) if payload.as_ref() == b"first"
                                        ));
                                        let second = app.block_on(io.write_all(b"second"))?;
                                        assert!(matches!(second,
                                            Err(Write::ConnectionClosed {
                                                payload: Some(payload)
                                            }) if payload.as_ref() == b"second"
                                        ));
                                        Ok(())
                                    },
                                )
                                .expect("the send-failure application must tear down cleanly");
                            ((driven).expect("send-failure fiber stalled"));
                        },
                    );
                    assert!(
                        result.is_ok(),
                        "the send-failure connector must build from its storage"
                    );

                    server
                        .finish()
                        .expect("the send-failure peer thread must join");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                self::finish,
            );
            assert!(
                reports
                    .events()
                    .expect("reclamation observations")
                    .is_empty(),
                "unexpected reclaimed operations: {reports:?}"
            );
        },
    )
    .expect("the send-failure peer thread must start");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
