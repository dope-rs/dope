use std::convert::Infallible;

pub mod common;

use common::{scenario::app::Empty, shutdown::try_create};
use dope_core::driver::life::{creation, recovery};
use dope_runtime::{executor::session::Completion, shutdown};
use dope_test::harness::scenario::TIMEOUT;

#[test]
fn delivers_readiness() {
    let mut create = creation::Attempt::new();
    let executor = try_create(&mut create, shutdown::Pair::new().expect("shutdown pair"));
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    let (executor, trigger) = executor.ok().expect("validated executor");
    {
        let mut observed = Vec::new();
        executor.run(|executor| {
            let result = executor.enter_with(
                |mut scope| {
                    let mut finish = Completion::new();
                    trigger.fire().expect("readiness shutdown");
                    let result = scope.run_quiesce(&mut finish, Empty {}, TIMEOUT);
                    assert!(
                        matches!(result, Ok(Ok(()))),
                        "readiness drive and teardown: {result:?}"
                    );
                },
                recovery::Policy::new(|event| observed.push(event), recover),
            );
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("executor completion");
            assert!(
                observed.len() <= 1,
                "one shutdown registration: {observed:?}"
            );
            assert!(
                observed
                    .iter()
                    .all(|event| matches!(event, dope_core::io::Reclaimed::Shutdown))
            );
        });
    }
}

#[test]
fn reclaims_shutdown() {
    for signaled in [false, true] {
        let mut create = creation::Attempt::new();
        let executor = try_create(&mut create, shutdown::Pair::new().expect("shutdown pair"));
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("executor creation");
        let (executor, trigger) = executor.ok().expect("validated executor");
        if signaled {
            trigger.fire().expect("ready shutdown trigger");
        }
        let mut observed = Vec::new();
        executor.run(|executor| {
            let result = executor.enter_with(
                |_| {},
                recovery::Policy::new(|event| observed.push(event), recover),
            );
            assert!(
                observed.len() <= 1,
                "one shutdown registration: {observed:?}"
            );
            assert!(
                observed
                    .iter()
                    .all(|event| matches!(event, dope_core::io::Reclaimed::Shutdown))
            );
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("executor completion");
        });
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
