use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_test::harness::{peer::TcpScript, scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{
    convert::Infallible,
    io::{Read, Write},
    net::Shutdown,
    time::Duration,
};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::{tcp::Tcp, wire::Identity},
    runtime::executor::drive,
};
use dope_fiber::{BlockOn as _, net::client::Plan};
use dope_test::{fibers, reclaim::Reports};

use crate::common::network::rounds::client::{App, Collect, MAX_CONN};

#[test]
fn keeps_following_writes() {
    let mut finish = Completion::new();
    const DELIVERED: &[u8] = b"keptoldnew";
    TcpScript::with(|stream| {
            stream
                .read_wait(Some(Duration::from_secs(2)))
                .expect("server read timeout must be configured");
            let mut req = [0; DELIVERED.len()];
            stream
                .read_exact(&mut req)
                .expect("server must receive every client write");
            assert_eq!(&req, DELIVERED);
        }, |server| {
        let addr = server.addr();

        let plan = Plan::<Tcp, Identity>::new(Capacity::fixed::<MAX_CONN>(), Identity);
        let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
        let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

        runtime.with_input(
            plan,
            |mut sess| {
                let plan = sess.storage();
                let store = (plan.try_build(sess.driver().storage)).expect("client port storage");
                let result = store.machine(
                    &mut sess,
                    |sess| sess.driver(),
                    |port, sess| {
                        let conn = store.handle();
                        let app = App { connector: port };

                        let driven = (sess.with_quiesce(
                            &mut finish,
                            app,
                            scenario::TIMEOUT,
                            |mut app| -> Result<(), drive::Error> {
                                let opened = app.block_on(dope_fiber::fiber!('_ => async move {
                                    conn.connect(addr).await
                                }))?;
                                let io = (opened).ok().expect("client connection must succeed");
                                let mut owner = Some(io);
                                let sent = app.block_on(dope_fiber::fiber!('_ => async move {
                                    let mut io = owner.take().expect("connection must be owned");
                                    let output = fibers::TEST.poll_once(io.write_all(b"discarded")).await;
                                    assert!(output.is_none());
                                    io.write_all(b"kept").await?;
                                    io.write_all(b"old").await?;
                                    io.write_all(b"new").await
                                }))?;
                                assert!(sent.is_ok(), "following writes must succeed");
                                Ok(())
                            },
                        ))
                        .expect("write test application must finish");
                        ((driven).expect("write fiber stalled"));
                    },
                );
                assert!(result.is_ok(), "connector machine must be constructed");
                server.finish().expect("write server must finish");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            self::finish,
        );
        assert!(
            reports.events().expect("reclamation observations").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }).expect("write server must start");
}

#[test]
fn writes_after_receive_eof() {
    let mut finish = Completion::new();
    TcpScript::with(|stream| {
            stream
                .write_all(b"tail")
                .expect("server tail must be written");
            stream
                .shutdown(Shutdown::Write)
                .expect("server receive direction must remain open");
            let mut response = [0; 11];
            stream
                .read_exact(&mut response)
                .expect("server must receive writes after EOF");
            assert_eq!(&response, b"firstsecond");
        }, |server| {
        let addr = server.addr();

        let plan = Plan::<Tcp, Identity>::new(Capacity::fixed::<MAX_CONN>(), Identity);
        let runtime = Setup::try_for_plan::<Balanced, _>(&plan).expect("runtime configuration");
        let mut reports = Reports::<_>::new(runtime.config(), [Id::fixed::<0>()]);

        runtime.with_input(plan,
            |mut sess| {
                let plan = sess.storage();
                let store = (plan.try_build(sess.driver().storage)).expect("client port storage");
                let result = store.machine(
                &mut sess,
                |sess| sess.driver(),
                |port, sess| {
                    let conn = store.handle();
                    let app = App { connector: port };

                    let driven =
                        (sess.with_quiesce(&mut finish, app, scenario::TIMEOUT, |mut app| -> Result<(), drive::Error> {
                            let opened = app.block_on(dope_fiber::fiber!('_ => async move {
                                conn.connect(addr).await
                            }))?;
                            let io = (opened).ok().expect("client connection must succeed");
                            let mut owner = Some(io);
                            app.block_on(dope_fiber::fiber!('_ => async move {
                                let mut io = owner.take().expect("connection must be owned");
                                let mut tail = Vec::new();
                                while let Some(chunk) = io.read().await.ok().expect("receive must succeed") {
                                    tail.extend_from_slice(&chunk.try_into_vec().expect("consume tail"));
                                }
                                assert_eq!(tail, b"tail");
                                assert!(matches!(io.read().await, Ok(None)));
                                io.write_all(b"first").await.ok().expect("first write after EOF");
                                io.write_all(b"second").await.ok().expect("second write after EOF");
                            }))
                        }))
                        .expect("half-close application must finish");
                    ((driven).expect("half-close fiber stalled"));
                },
            );
                assert!(result.is_ok(), "connector machine must be constructed");
                server.finish().expect("half-close server must finish");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            self::finish,
        );
        assert!(
            reports.events().expect("reclamation observations").is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }).expect("half-close server must start");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
