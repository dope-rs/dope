use dope::{
    core::{driver::life::recovery, io::Reclaimed},
    runtime::executor::session::Completion,
};
use dope_fiber::task::time::{
    budget::{Budget, Elapsed},
    sleep::{Error, Sleep},
};
use dope_test::harness::scenario;
use slots::Slots;

use crate::common::runtime::Empty;
pub mod common;

use std::{
    convert::Infallible,
    pin::pin,
    task::Poll,
    time::{Duration, Instant},
};

use dope::core::driver::settings::demands::slots;
use dope_fiber::{
    BlockOn,
    abi::{Pending, Ready, poll_fn},
    task::time::{budget, clock::Clock},
};

use crate::common::runtime::{self, Setup};

#[test]
fn rejects_long_sleep() {
    let mut reports = Vec::new();

    Setup::throughput()
        .timers(Slots::fixed::<1>())
        .with_session(
            |mut sess| {
                let timer = sess.driver().driver_ref().timer();
                assert!(
                    matches!(
                        Sleep::new(timer, Duration::MAX),
                        Err(Error::Duration {
                            actual: Duration::MAX
                        })
                    ),
                    "an unrepresentable deadline must be rejected"
                );
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            check_scope,
        );
    check_reports(&reports);
}

#[test]
fn reuses_timer_slot() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::throughput()
        .timers(Slots::fixed::<1>())
        .with_session(
            |mut sess| {
                let timer = sess.driver().driver_ref().timer();
                let done = sess.with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                    app.block_on(dope_fiber::fiber!('_ => async move {
                        let started = Clock::now().await;
                        {
                            let first = Budget::try_new(
                                timer,
                                started,
                                Duration::from_secs(1),
                            );
                            assert!(first.is_ok(), "reserve the only timer slot");
                            assert!(matches!(
                                Budget::try_new(timer, started, Duration::from_secs(1)),
                                Err(budget::Error::Capacity(_))
                            ));
                            assert!(matches!(
                                Budget::try_new(timer, started, Duration::MAX),
                                Err(budget::Error::Range(_))
                            ));
                        }

                        let mut budget = (Budget::try_new(
                            timer,
                            started,
                            Duration::from_secs(1),
                        )).expect("reuse the released timer slot");
                        drop(budget.sleep(Duration::ZERO));
                        let first = budget.timeout(Ready::new(1_u8)).await;
                        assert!(matches!(first, Ok(1)));
                        let second = budget.timeout(Ready::new(2_u8)).await;
                        assert!(matches!(second, Ok(2)));
                    }))
                });
                assert!(matches!(done, Ok(Ok(()))), "application drive and teardown");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            check_scope,
        );
    check_reports(&reports);
}

#[test]
fn checks_bound_first() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::throughput()
        .timers(Slots::fixed::<1>())
        .with_session(|mut sess| {
            let timer = sess.driver().driver_ref().timer();
            let done = sess.with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                app.block_on(dope_fiber::fiber!('_ => async move {
                    let started = Clock::now().await;
                    {
                        let mut budget = (Budget::try_new(timer, started, Duration::ZERO)).expect("reserve timer slot");
                        assert_eq!(
                            budget.limit(started, Duration::ZERO).checkpoint().check(),
                            Err(Elapsed::Budget),
                        );
                        assert_eq!(
                            budget.scope(started, Duration::MAX).checkpoint().check(),
                            Err(Elapsed::Budget),
                        );
                        let expired_timeout = budget.timeout(Ready::new(7_u8)).await;
                        assert!(matches!(expired_timeout, Err(Elapsed::Budget)));
                        for duration in [Duration::ZERO, Duration::MAX] {
                            let expired_sleep = budget.sleep(duration).await;
                            assert!(matches!(expired_sleep, Err(Elapsed::Budget)));
                        }
                    }

                    let mut budget = (Budget::try_new(
                        timer,
                        Clock::now().await,
                        Duration::from_secs(1),
                    )).expect("reserve timer slot");
                    {
                        let mut limited = budget.limit(Clock::now().await, Duration::ZERO);
                        let limited_timeout =
                            limited.timeout(Pending::<Infallible>::new()).await;
                        assert!(matches!(limited_timeout, Err(Elapsed::Limit)));
                        for duration in [Duration::ZERO, Duration::MAX] {
                            let limited_sleep = limited.sleep(duration).await;
                            assert!(matches!(limited_sleep, Err(Elapsed::Limit)));
                        }
                    }
                    let active_timeout = budget.timeout(Ready::new(9_u8)).await;
                    assert!(matches!(active_timeout, Ok(9)));
                    let unlimited_timeout = budget
                        .timeout_for(Duration::MAX, Ready::new(10_u8))
                        .await;
                    assert!(matches!(unlimited_timeout, Ok(10)));
                    let active_sleep = budget.sleep(Duration::ZERO).await;
                    assert!(matches!(active_sleep, Ok(())));
                    let limited_timeout = budget
                        .timeout_for(Duration::ZERO, Pending::<Infallible>::new())
                        .await;
                    assert!(matches!(limited_timeout, Err(Elapsed::Limit)));
                }))
            });
            assert!(matches!(done, Ok(Ok(()))), "application drive and teardown");
        }, recovery::Policy::new(|event| reports.push(event), recover), check_scope);
    check_reports(&reports);
}

#[test]
fn checks_ready_child_time() {
    let mut finish = Completion::new();
    let mut reports = Vec::new();

    Setup::throughput()
        .timers(Slots::fixed::<1>())
        .with_session(
            |mut sess| {
                let done = sess.with_quiesce(&mut finish, Empty, scenario::TIMEOUT, |mut app| {
                    app.block_on(poll_fn(|mut context| {
                        let driver = context.as_mut().driver();
                        let now = driver.driver_ref().clock().now();
                        let timer = driver.driver_ref().timer();
                        let mut budget = Budget::try_new(timer, now, Duration::from_secs(60))
                            .expect("reserve the only timer slot");
                        let mut scope = budget.scope(now, Duration::from_nanos(1));
                        let check = scope.checkpoint();
                        let child = poll_fn(move |_| {
                            let started = Instant::now();
                            loop {
                                if let Err(elapsed) = check.check() {
                                    break Poll::Ready(Err::<(), _>(elapsed));
                                }
                                assert!(
                                    started.elapsed() < Duration::from_secs(1),
                                    "deadline check did not expire"
                                );
                            }
                        });
                        let mut timeout = pin!(dope_fiber::abi::Task::new(scope.timeout(child)));
                        assert!(matches!(
                            context.poll(timeout.as_mut()),
                            Poll::Ready(Ok(Err(Elapsed::Limit)))
                        ));
                        Poll::Ready(())
                    }))
                });
                assert!(matches!(done, Ok(Ok(()))), "application drive and teardown");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            check_scope,
        );
    check_reports(&reports);
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

fn check_scope(result: Result<(), runtime::Error<'_>>) {
    result.expect("driver lifecycle");
}

fn check_reports(reports: &[Reclaimed]) {
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}
