use dope::{core::driver::life::recovery, runtime::executor::session::Completion};
use dope_core::driver::route::KeyTag;
use dope_test::harness::{peer::TcpScript, scenario};

use crate::common::runtime::{self, Setup};
pub mod common;

use std::{array, convert::Infallible, io::Read as _};

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::{tcp::Tcp, wire::Identity},
};
use dope_fiber::{
    BlockOn as _,
    net::{client::Plan, error::Write},
};
use dope_test::reclaim::Reports;

use crate::common::network::rounds::client::shutdown::*;

#[test]
fn shutdown_preserves_write() {
    let mut finish = Completion::new();
    TcpScript::with_many::<4, _>(
        |streams| {
            for stream in streams {
                let mut bytes = Vec::new();
                stream
                    .read_to_end(&mut bytes)
                    .expect("shutdown closes every peer");
                assert!(bytes.is_empty());
            }
        },
        |server| {
            let addr = server.addr();
            let plans = (
                Plan::<Tcp, Identity, KeyTag<0>>::new(Capacity::fixed::<1>(), Identity),
                Plan::<Tcp, Identity, KeyTag<1>>::new(Capacity::fixed::<257>(), Identity),
            );
            let runtime =
                Setup::try_for_plan::<Balanced, _>(&plans).expect("runtime configuration");
            let mut reports =
                Reports::<_>::new(runtime.config(), [Id::fixed::<0>(), Id::fixed::<1>()]);
            runtime.with_input(
                plans,
                |mut sess| {
                    let plans = sess.storage();
                    let first = plans
                        .0
                        .try_build(sess.driver().storage)
                        .expect("first port");
                    let second = plans
                        .1
                        .try_build(sess.driver().storage)
                        .expect("second port");
                    let result = first.machine(
                        &mut sess,
                        |sess| sess.driver(),
                        |one, sess| {
                            let result = second.machine(
                                sess,
                                |sess| sess.driver(),
                                |two, sess| {
                                    let handle1 = first.handle();
                                    let handle2 = second.handle();
                                    let held = sess.with_quiesce(
                                        &mut finish,
                                        App {
                                            first: one,
                                            second: two,
                                        },
                                        scenario::TIMEOUT,
                                        |mut app| {
                                            let first = app
                                                .block_on(handle1.connect(addr))
                                                .expect("first connect progresses")
                                                .ok()
                                                .expect("first connects");
                                            let second = array::from_fn::<_, 3, _>(|_| {
                                                app.block_on(handle2.connect(addr))
                                                    .expect("second connect progresses")
                                                    .ok()
                                                    .expect("second connects")
                                            });
                                            let writer = dope_fiber::fiber!('_ => async move {
                                                let mut first = first;
                                                first.write_all(PAYLOAD).await
                                            });
                                            let (writer, poll) = app
                                                .block_on(retain_pending(writer))
                                                .expect("write is staged");
                                            assert!(poll.is_pending(), "write remains pending");
                                            (writer, second)
                                        },
                                    );
                                    assert!(
                                        held.is_ok(),
                                        "shutdown completes with both domains retained"
                                    );
                                    let (mut writer, _second) = held.ok().expect("shutdown");
                                    let wrote = sess
                                        .with_app(&mut finish, runtime::Empty, |mut app| {
                                            app.block_on(writer.as_mut())
                                        })
                                        .ok()
                                        .expect("completed application")
                                        .expect("retained write progresses");
                                    let payload = match wrote.expect_err("retained write fails") {
                                        Write::ConnectionClosed { payload } => payload,
                                        _ => None,
                                    }
                                    .expect("shutdown preserves the exact write failure");
                                    assert_eq!(payload.as_ref(), PAYLOAD);
                                },
                            );
                            assert!(result.is_ok(), "second machine");
                        },
                    );
                    assert!(result.is_ok(), "first machine");
                    server.finish().expect("shutdown peers finish");
                },
                recovery::Policy::new(|event| reports.push(event), recover),
                self::finish,
            );
            assert!(
                reports
                    .events()
                    .expect("reclamation observations")
                    .is_empty(),
                "unexpected shutdown reclamation: {reports:?}"
            );
        },
    )
    .expect("shutdown peers");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn finish(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}
