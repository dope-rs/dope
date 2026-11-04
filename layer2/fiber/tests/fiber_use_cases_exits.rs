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
use common::recover;

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
    let executor = Setup::for_profile::<Tasks>()
        .try_executor(&mut create)
        .unwrap_or_else(|error| panic!("shutdown test executor: {:?}", error.kind()));
    let executor = executor
        .shutdown(source)
        .expect("shutdown source registration");
    let mut reports = Vec::new();

    executor.run(|executor| {
        let entered = executor.enter_with(
            |mut session| {
                let result = session
                    .with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                        app.block_on(Pending::<()>::default())
                    })
                    .expect("shutdown application teardown failed");
                let error = result.err().expect("pending app must be interrupted");
                assert!((error.kind()) == (Kind::Shutdown));
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );

        entered.unwrap_or_else(|error| panic!("shutdown driver domain: {:?}", error.cause()));
    });
}
