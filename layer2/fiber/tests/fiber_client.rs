use std::convert::Infallible;

use dope::{
    core::driver::{life::recovery, settings::Config, storage::Value},
    net::{link::event::ConnectFault, wire::domain::Local},
    runtime::executor::session::Completion,
};
use dope_fiber::net::client::ConnectError;
use dope_test::harness::scenario;
pub mod common;

use dope::{
    core::driver::route::{Id, table::conn::Capacity},
    manifold::timing::Balanced,
    net::{link::event, wire::Identity},
    runtime::executor::Executor,
};
use dope_fiber::{BlockOn as _, net::client};
use dope_test::reclaim::Reports;

use crate::common::network::clients::*;

#[test]
fn failed_dial_reuses_slot() {
    let mut finish = Completion::new();
    let cfg = Config::for_profile::<Balanced>();
    use dope::core::driver::life::creation::Attempt;
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, cfg);
    assert!(executor.is_ok(), "executor construction failed");
    let Some(executor) = executor.ok() else {
        return;
    };
    let mut reports = Reports::<_>::new(cfg, [Id::fixed::<0>()]);

    executor
        .with_plan(Value::new(client::Plan::<Transport, Identity>::new(
            Capacity::fixed::<1>(),
            Identity,
        ))).run(|executor| {
    let result = executor.enter_with(
            |mut session| {
                let plan = session.storage();
                let storage =
                    (plan.try_build(session.driver().storage)).expect("client port storage");
                let handle = storage.handle();
                let built = storage.machine_in(
                    Local::new(Identity),
                    &mut session,
                    |session| session.driver(),
                    |conn, session| {
                        session.with_quiesce(&mut finish, App { conn }, scenario::TIMEOUT, |mut app| {
                            for _ in 0..2 {
                                let result =
                                    (app.block_on(dope_fiber::fiber!('_ => async move {
                                        handle.connect(Addr).await
                                    })))
                                    .expect("connect fiber stalled");
                                assert!(
                                    matches!(
                                        result,
                                        Err(ConnectError::Transport(ConnectFault::Transport(
                                            event::ConnectError::Connect(_)
                                        )))
                                    ),
                                    "unsupported target must report the failed connect operation"
                                );
                            }
                        })
                    },
                );
                assert!(
                    matches!(built, Ok(Ok(()))),
                    "client machine or teardown failed"
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
    assert!(
        reports.events().expect("reclamation observations").is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert!(result.is_ok(), "client route failed");
    });
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
