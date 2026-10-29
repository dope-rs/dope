use std::convert::Infallible;

use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_test::harness::{peer::AcceptScript, scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::{BlockOn as _, net::client::Plan};
use dope_test::{fibers, reclaim::Reports};

use crate::common::network::rounds::client::{App, Ping, try_reply};

#[test]
fn reuses_cancelled_slot() {
    let mut finish = Completion::new();
    let reply: &[u8] = b"PONG-after-cancel";
    AcceptScript::with(
        |peer| try_reply(peer, reply, 2),
        |server| {
            let addr = server.addr();

            let plan = Plan::<Tcp, Identity>::new(Capacity::fixed::<1>(), Identity);
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
                            let driven = (sess.with_quiesce(
                                &mut finish,
                                app,
                                scenario::TIMEOUT,
                                |mut app| {
                                    app.block_on(dope_fiber::fiber!('_ => async move {
                                    let output = fibers::TEST.poll_once(conn.connect(addr)).await;
                                    assert!(output.is_none());
                                }))?;
                                    let opened =
                                        app.block_on(dope_fiber::fiber!('_ => async move {
                                            conn.connect(addr).await
                                        }))?;
                                    let io = (opened).ok().expect(
                                        "the second connection must open after cancellation",
                                    );
                                    Ping::try_ping(&mut app, io).map(|result| {
                                        assert_eq!(
                                            result.ok().expect("complete ping roundtrip"),
                                            reply
                                        )
                                    })
                                },
                            ))
                            .expect("the cancelled-slot application must tear down cleanly");
                            ((driven).expect("cancel roundtrip fiber stalled"));
                        },
                    );
                    assert!(
                        result.is_ok(),
                        "the cancelled-slot connector must build from its storage"
                    );
                    server
                        .finish()
                        .expect("the reply-server thread must join")
                        .expect("the reply server must complete both accepts");
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

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
