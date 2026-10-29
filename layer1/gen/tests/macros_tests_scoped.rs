use std::convert::Infallible;

use dope_test::harness::scenario::TIMEOUT;
use engine::{
    core::{
        driver::life::{
            creation::Attempt,
            recovery::{Policy, Recovery},
        },
        io::Reclaimed,
    },
    runtime::executor::session::Completion,
};

use crate::common::native;
pub mod common;

use std::marker::PhantomData;

use fibers::{
    BlockOn as _, abi,
    abi::{Pending, Ready},
};

use crate::common::{fibers::Scoped, macros};

#[test]
fn repeated_awaits() {
    {
        let pending = wait_repeated();
        assert_output(&pending);
    }
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut sess| {
                let result = sess.with_quiesce(
                    &mut report,
                    macros::Dispatcher::default(),
                    TIMEOUT,
                    |mut app| {
                        let output = app
                            .block_on(sum_repeated())
                            .expect("repeated fiber stalled");
                        assert_eq!(output, 10)
                    },
                );
                assert!(result.is_ok(), "application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn impl_lifetime() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut sess| {
                let result = sess.with_quiesce(
                    &mut report,
                    macros::Dispatcher::default(),
                    TIMEOUT,
                    |mut app| {
                        let value = String::from("application-scope");
                        let scoped = Scoped {
                            value: &value,
                            driver: PhantomData,
                        };
                        let fiber = scoped.ready();
                        {
                            let output = app.block_on(fiber).expect("scoped fiber stalled");
                            assert_eq!(output, value)
                        }
                    },
                );
                assert!(result.is_ok(), "application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[fibers::fiber_fn('d, crate = ::fibers)]
async fn sum_repeated<'d>() -> usize {
    let mut sum = 0usize;
    for value in 1usize..=4 {
        sum += Ready::new(value).await;
    }
    sum
}

#[fibers::fiber_fn('d, crate = ::fibers)]
async fn wait_repeated<'d>() -> usize {
    loop {
        Pending::<()>::new().await;
    }
}

fn assert_output<'d>(_: &impl abi::Fiber<'d, Output = usize>) {}

fn shutdown(event: Reclaimed) {
    assert!(
        matches!(event, Reclaimed::Shutdown),
        "unexpected operation: {event:?}"
    );
}

fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
