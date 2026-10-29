use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_test::harness::{
    peer::{AcceptScript, TcpScript},
    scenario,
};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{convert::Infallible, net::Shutdown};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::tcp::Tcp,
    runtime::executor::drive,
};
use dope_fiber::{
    BlockOn as _,
    net::{
        client::Plan,
        error::read::{Buffer, Chunks, Local, Read},
    },
};
use dope_test::reclaim::Reports;

use crate::common::network::rounds::client::{
    Collect,
    fault::{Error, read::*},
    try_reply,
};

#[test]
fn emits_plain_first() {
    let mut finish = Completion::new();
    AcceptScript::with(
        |peer| try_reply(peer, b"x", 1),
        |server| {
            let addr = server.addr();

            let plan = Plan::<Tcp, Wire>::new(Capacity::fixed::<1>(), ());
            let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
            let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

            runtime.with_input(
                plan,
                |mut sess| {
                    let plan = sess.storage();
                    let store = plan
                        .try_build(sess.driver().storage)
                        .expect("client port storage");
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
                                            .expect("the plain-first connection must open");
                                        let sent = app.block_on(io.write_all(b"ping"))?;
                                        assert!(
                                            sent.is_ok(),
                                            "the plain-first connection must write its ping"
                                        );
                                        {
                                            let read = app.block_on(io.read())?;
                                            let lease = (read)
                                        .ok()
                                        .expect("the wire must emit its plain bytes before failure")
                                        .expect(
                                            "the wire must emit its plain bytes before failure",
                                        );
                                            assert_eq!(
                                                Collect::try_into_vec(lease)
                                                    .expect("consume received bytes"),
                                                b"x"
                                            );
                                        }
                                        {
                                            let read = app.block_on(io.read())?;
                                            assert!(matches!(read,
                                                Err(Read::Wire(error))
                                                    if matches!(error.causes(), (Error::Protocol(
                                                        "domain-borrowed terminal failure"
                                                    ), None))
                                            ));
                                        }
                                        let closed = app.block_on(io.read())?;
                                        assert!(
                                            matches!(closed, Ok(None)),
                                            "the plain-first failure must be emitted only once"
                                        );
                                        Ok(())
                                    },
                                )
                                .expect("the plain-first application must tear down cleanly");
                            ((driven).expect("plain-first fiber stalled"));
                        },
                    );
                    assert!(
                        result.is_ok(),
                        "the plain-first connector must build from its storage"
                    );

                    let result = server.finish();
                    let result = (result).expect("the plain-first reply-server thread must join");
                    assert!(
                        result.is_ok(),
                        "the plain-first reply server must complete its accept"
                    );
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
    .expect("reply server");
}

#[test]
fn keeps_eof_error() {
    let mut finish = Completion::new();
    TcpScript::with(
        |stream| {
            assert!(
                stream.shutdown(Shutdown::Both).is_ok(),
                "the EOF peer must close both socket directions"
            );
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
                    let store = plan
                        .try_build(sess.driver().storage)
                        .expect("client port storage");
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
                                        let mut io =
                                            (opened).ok().expect("the EOF connection must open");
                                        {
                                            let read = app.block_on(io.read())?;
                                            assert!(matches!(read,
                                                Err(Read::Wire(error))
                                                    if matches!(error.causes(), (Error::Truncated(
                                                        "domain-borrowed terminal failure"
                                                    ), None))
                                            ));
                                        }
                                        let closed = app.block_on(io.read())?;
                                        assert!(
                                            matches!(closed, Ok(None)),
                                            "the EOF failure must be emitted only once"
                                        );
                                        Ok(())
                                    },
                                )
                                .expect("the EOF application must tear down cleanly");
                            ((driven).expect("EOF fiber stalled"));
                        },
                    );
                    assert!(
                        result.is_ok(),
                        "the EOF connector must build from its storage"
                    );

                    assert!(server.finish().is_ok(), "the EOF peer thread must join");
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
    .expect("the EOF peer thread must start");
}

#[test]
fn typed_capacities() {
    let error: Read<()> = Local::BufferedBytesTooLarge {
        actual: Buffer::new(17),
        maximum: Buffer::new(16),
    }
    .into();
    assert!(matches!(
        error,
        Read::BufferedBytesTooLarge { actual, maximum }
            if actual.get() == 17 && maximum.get() == 16
    ));

    let error: Read<()> = Local::ChunkCountTooLarge {
        actual: Chunks::new(5),
        maximum: Chunks::new(4),
    }
    .into();
    assert!(matches!(
        error,
        Read::ChunkCountTooLarge { actual, maximum }
            if actual.get() == 5 && maximum.get() == 4
    ));
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
