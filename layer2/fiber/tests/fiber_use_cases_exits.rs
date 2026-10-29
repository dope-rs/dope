use dope::{
    core::driver::life::{creation::Attempt, recovery},
    runtime::{
        executor::{drive::Kind, session::Completion},
        shutdown::Pair,
    },
};
use dope_test::harness::scenario;

use crate::common::runtime::profile::Tasks;
pub mod common;

use std::convert::Infallible;

use dope_fiber::{BlockOn, abi::Pending};

use crate::common::runtime::{Empty, Setup};

#[test]
#[deny(unsafe_code)]
fn finishes_interrupted_app() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let pair = Pair::new().expect("shutdown pair creation");
    let (source, trigger) = pair.split();
    assert!(trigger.fire().is_ok(), "fire shutdown");
    let executor = Setup::for_profile::<Tasks>().try_executor(&mut create);
    assert!(executor.is_ok(), "shutdown test executor creation failed");
    let Ok(executor) = executor else {
        return;
    };
    let executor = executor.shutdown(source);
    assert!(executor.is_ok(), "shutdown source registration failed");
    let Ok(executor) = executor else {
        return;
    };
    let mut reports = Vec::new();

    executor.run(|executor| {
        let entered = executor.enter_with(
            |mut session| {
                let result = session
                    .with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                        app.block_on(Pending::<()>::default())
                    })
                    .expect("shutdown application teardown failed");
                assert!(result.is_err(), "pending app must be interrupted");
                let Err(error) = result else {
                    return;
                };
                assert!((error.kind()) == (Kind::Shutdown));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        assert!(entered.is_ok(), "shutdown driver domain failed");
        let Ok(()) = entered else {
            return;
        };
    });
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
