use std::{convert::Infallible, pin::pin, task::Poll, time::Duration};

use dope_core::driver::{
    life::{creation::Attempt, recovery},
    retained,
    route::{Epoch, KeyTag, indices::SlotIndex},
    schedule,
    settings::{self, demands, demands::slots::Slots},
};
use dope_fiber::{
    abi::{Pending, Ready, Task},
    context::{Context, PollResult},
    task::time::sleep,
};
use dope_runtime::executor::{Executor, session::Completion};
use dope_test::{
    alloc::Tracker,
    fibers::{Harness, Run, Yield},
    harness::{observe::Trace, signals::Gate},
    scope::{Driver, Error},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn one_credit_progress() {
    let mut reports = Vec::new();
    Driver::new(settings::Config::default()).with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let root = core
                    .ready()
                    .make_slot(
                        core.targets::<KeyTag<0>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch(),
                    )
                    .expect("ready slot");
                let owner = retained::raw::Owner::from_driver(core);
                let mut driver = retained::Context::new(context, owner);
                {
                    let turn = control.begin();
                    let mut fiber = pin!(Task::new(Yield::new()));
                    let (poll, alloc) = Tracker::usage(|| {
                        Context::with_target(
                            root.target(),
                            turn.turn().app(),
                            driver.reborrow(),
                            |mut context| context.as_mut().try_poll(fiber.as_mut()),
                        )
                    });
                    assert!(matches!(poll, PollResult::Polled(Poll::Pending)));
                    assert_eq!(alloc, (0, 0));
                    let woken = Trace::<_, 1>::new();
                    assert_eq!(
                        turn.ready()
                            .drain(|target| woken.record(target))
                            .map_or(0, |count| count.get()),
                        1
                    );
                    assert!(
                        woken
                            .matches(&[core
                                .targets::<KeyTag<0>>()
                                .bind(SlotIndex::ZERO, Epoch::INITIAL)
                                .dispatch()])
                            .expect("wake observations")
                    );
                }
                let harness = Harness::new(Duration::ZERO);
                let mut ready = pin!(Task::new(harness.within(harness.poll_once(Ready::new(7)))));
                {
                    let turn = control.begin();
                    let output = turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        Context::with_target(
                            root.target(),
                            share.work(),
                            driver.reborrow(),
                            |context| context.poll(ready.as_mut()),
                        )
                    });
                    assert_eq!(output, Poll::Ready(Ok(Some(Some(7)))));
                }

                let mut deadline = pin!(Task::new(harness.within(Pending::<()>::new())));
                for expected in [Poll::Pending, Poll::Ready(Ok(None))] {
                    let turn = control.begin();
                    let output = turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        Context::with_target(
                            root.target(),
                            share.work(),
                            driver.reborrow(),
                            |context| context.poll(deadline.as_mut()),
                        )
                    });
                    assert_eq!(output, expected);
                }
            });
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", failure.cause());
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(Error::cause)
                .expect("driver lifecycle");
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reports_timer_errors() {
    for (timers, guard, expected) in [
        (
            Slots::fixed::<1>(),
            Duration::MAX,
            sleep::Error::Duration {
                actual: Duration::MAX,
            },
        ),
        (Slots::ZERO, Duration::from_secs(1), sleep::Error::Capacity),
    ] {
        let mut attempt = Attempt::new();
        let mut reports = Vec::new();
        let config = settings::Config::default();
        let schedule = settings::Schedule::fixed::<0, 0>()
            .resolve(demands::Schedule::new(config.schedule().ready(), timers));
        let executor = Executor::new(&mut attempt, config.set_schedule(schedule))
            .unwrap_or_else(|error| panic!("executor: {:?}", error.kind()));
        executor.run(|executor| {
            let result = executor.enter_with(
                |mut scope| {
                    let trace = Trace::<_, 16>::new();
                    let mut finish = Completion::new();
                    let done = scope.with_app(&mut finish, &trace, |mut app| {
                        let harness = Harness::new(guard);
                        assert!(matches!(
                            harness.try_run(&mut app, Pending::<()>::new()),
                            Err(Run::Timer(actual)) if actual == expected
                        ));
                        let gate = Gate::new();
                        assert!(matches!(
                            harness.try_wait(&mut app, &gate, 1),
                            Err(Run::Timer(actual)) if actual == expected
                        ));
                    });
                    assert!(matches!(done, Ok(())), "application drive and teardown");
                    assert!(trace.values().is_ok(), "activation trace");
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", failure.cause());
                    },
                ),
            );
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver lifecycle");
        });
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
    }
}
