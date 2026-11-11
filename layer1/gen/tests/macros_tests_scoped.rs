use dope_test::harness::scenario::TIMEOUT;
use engine::runtime::executor::session::Completion;

use crate::common::native;
pub mod common;

use std::marker::PhantomData;

use fibers::{BlockOn as _, abi::Ready};

use crate::common::{fibers::Scoped, macros};

#[test]
fn repeated_awaits() {
    let mut report = Completion::new();
    native::run(|mut sess| {
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
    });
}

#[test]
fn impl_lifetime() {
    let mut report = Completion::new();
    native::run(|mut sess| {
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
