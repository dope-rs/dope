use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_test::{fibers::TEST, harness::scenario::TIMEOUT};
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

use crate::common::{
    native,
    owners::{self, Order},
};
pub mod common;

use fibers::{BlockOn, abi::Ready};

use crate::common::fibers::*;

#[test]
fn storage_reuses_maximum() {
    let one = fibers::fiber!('_, crate = ::fibers => async move {
        {
            let storage: [u8; 4096] = [1; 4096];
            Ready::new(()).await;
            storage[0] as usize
        }
    });
    let series = fibers::fiber!('_, crate = ::fibers => async move {
        let first = {
            let storage: [u8; 4096] = [1; 4096];
            Ready::new(()).await;
            storage[0] as usize
        };
        let second = {
            let storage: [u8; 4096] = [2; 4096];
            Ready::new(()).await;
            storage[0] as usize
        };
        first + second
    });
    assert!(std::mem::size_of_val(&series) <= std::mem::size_of_val(&one) + 64);
}

#[test]
fn nested_await_completes() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    let executor = executor.ok().expect("created executor");
    executor.run(|executor| {
        let domain = executor.enter_with(
            |mut session| {
                let retained = Rc::new(());
                let captured = Rc::clone(&retained);
                let fiber = fibers::fiber!('_, crate = ::fibers => async move {
                    let __dope_fiber = 7usize;
                    let mut count = 0;
                    let value = Ready::new({
                        count += 1;
                        Ready::new(__dope_fiber).await
                    }).await;
                    assert_eq!(Rc::strong_count(borrowed(&{
                        count += 1;
                        Rc::clone(&captured)
                    }).await), 3);
                    assert_eq!(Rc::strong_count(&captured), 2);
                    (value, count, __dope_fiber)
                });
                let result =
                    session.with_quiesce(&mut report, App { dummy: Dummy }, TIMEOUT, |mut app| {
                        let order = Cell::new(0);
                        let build = || {
                            owners::patterns(
                                1,
                                (Order(&order, 1), Order(&order, 2)),
                                Order(&order, 3),
                            )
                        };
                        let stopped = app
                            .block_on(TEST.poll_once(build()))
                            .expect("borrowed patterns suspend");
                        assert_eq!(stopped, None);
                        assert_eq!(order.replace(0), 312);
                        let output = app.block_on(build()).expect("borrowed patterns complete");
                        assert_eq!(output, (2, 0));
                        assert_eq!(order.get(), 342);
                        app.block_on(fiber)
                    });
                assert_eq!(Rc::strong_count(&retained), 1);
                result
            },
            Policy::new(shutdown, recover),
        );
        domain
            .as_ref()
            .map_err(|error| error.cause())
            .expect("runtime completion");
        let value = domain
            .ok()
            .expect("completed runtime")
            .expect("fiber runtime completed")
            .expect("fiber completed");
        assert_eq!(value, (7, 2, 7));
    });
}

#[test]
fn std_macros_lower() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    let executor = executor.ok().expect("created executor");
    executor.run(|executor| {
        let domain = executor.enter_with(
            |mut session| {
                let fiber = fibers::fiber!('_, crate = ::fibers => async move {
                    let values = vec![
                        Ready::new(1usize).await,
                        Ready::new(2usize).await,
                        3,
                    ];
                    let formatted = format!("{}", Ready::new(7usize).await);
                    let matched = matches!(Ready::new(Some(3usize)).await, Some(3));
                    ::core::assert!(::core::matches!(
                        Ready::new(Some(2usize)).await,
                        Some(value) if vec![Ready::new(value).await; Ready::new(2usize).await]
                            == [2, 2]
                    ));
                    (values, formatted, matched)
                });
                session.with_quiesce(&mut report, App { dummy: Dummy }, TIMEOUT, |mut app| {
                    app.block_on(fiber)
                })
            },
            Policy::new(shutdown, recover),
        );
        domain
            .as_ref()
            .map_err(|error| error.cause())
            .expect("runtime completion");
        let (values, text, matched) = domain
            .ok()
            .expect("completed runtime")
            .expect("fiber runtime completed")
            .expect("fiber completed");
        assert_eq!(values, [1, 2, 3]);
        assert_eq!(text, "7");
        assert!(matched);
    });
}

#[test]
fn clone_keeps_local_owned() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    let executor = executor.ok().expect("created executor");
    executor.run(|executor| {
        let domain = executor.enter_with(
            |mut session| {
                let fiber = fibers::fiber!('_, crate = ::fibers => async move {
                    let ranking = Ranking;
                    let user = String::from("alice").into_bytes();
                    match ranking.rank(user.clone()).await {
                        Ok(Some(rank)) => (user, rank),
                        Ok(None) | Err(()) => (user, 0),
                    }
                });
                session.with_quiesce(&mut report, App { dummy: Dummy }, TIMEOUT, |mut app| {
                    app.block_on(fiber)
                })
            },
            Policy::new(shutdown, recover),
        );
        domain
            .as_ref()
            .map_err(|error| error.cause())
            .expect("runtime completion");
        let (user, rank) = domain
            .ok()
            .expect("completed runtime")
            .expect("fiber runtime completed")
            .expect("fiber completed");
        assert_eq!(user, b"alice");
        assert_eq!(rank, 5);
    });
}

fn shutdown(event: Reclaimed) {
    assert!(
        matches!(event, Reclaimed::Shutdown),
        "unexpected operation: {event:?}"
    );
}

fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
