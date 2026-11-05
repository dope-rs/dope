use dope_core::driver::{
    life::{creation::Attempt, recovery},
    route::KeyTag,
};
use dope_runtime::executor::session::Completion;
use dope_test::harness::scenario::{TIMEOUT, run, run::Task as _};
use o3::base::time::Positive;
pub mod common;

use std::{cell::Cell, convert::Infallible, pin, rc::Rc, task, time};

use dope_core::driver::{route, route::indices::SlotIndex, schedule::ready::wake::Waker};
use dope_runtime::{executor::drain, process::lifecycle, shutdown};

use crate::common::{
    runtime::executor::shutdown::*,
    scenario::{app::Empty, dispatch, rt::Configured},
};

#[test]
fn installed_drain() {
    drain(
        Positive::from_millis::<50>(),
        3,
        drain::Outcome::Quiesced,
        false,
    );
}

#[test]
fn drain_deadline_resumes() {
    drain(
        Positive::from_millis::<50>(),
        3,
        drain::Outcome::Quiesced,
        true,
    );
}

#[test]
fn deadline_forces_drain() {
    let config = Positive::from_millis::<1>();
    drain(config, usize::MAX, drain::Outcome::Forced, false);
}

#[test]
fn run_stops_once() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let stops = Rc::new(Cell::new(Some(0)));
    let ends = Rc::new(Cell::new(Some(0)));
    let pair = (shutdown::Pair::new()).expect("shutdown pair");
    let (source, trig) = pair.split();
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    assert!(trig.fire().is_ok(), "fire runtime shutdown");
                    let request = sess.request(
                        &mut finish,
                        dispatch::Builder::new(Count {
                            stops: Rc::clone(&stops),
                            ends: Rc::clone(&ends),
                            root: None,
                        })
                        .probe::<KeyTag<0>>(),
                    );
                    request
                        .as_ref()
                        .map_err(|error| error.cause())
                        .expect("application completion");
                    let _request: shutdown::Requested<_> = request
                        .ok()
                        .expect("validated application")
                        .expect("application drive");
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(stops.get(), Some(1));
    assert_eq!(ends.get(), Some(1));
}

#[test]
fn quiescent_precedes_park() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let calls = Rc::new(Cell::new(Some(0)));
    let pair = (shutdown::Pair::new()).expect("shutdown pair");
    let (source, trig) = pair.split();
    assert!(trig.fire().is_ok(), "pre-install shutdown");
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    sess.run_quiesce(
                        &mut finish,
                        Storm {
                            calls: Rc::clone(&calls),
                        },
                        TIMEOUT,
                    )
                    .expect("application teardown")
                    .expect("application drive");
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(calls.get(), Some(0));
}

#[test]
fn ready_install_shutdown() {
    let (main, worker) = lifecycle::Main::try_pair().expect("ready shutdown pair");
    let ((ready, stopped), outcome) = run::Entry(move || {
        worker.run(|source| {
            let mut finish = Completion::new();
            let mut create = Attempt::new();
            let mut reports = Vec::new();

            let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
            executor
                .as_ref()
                .map_err(|error| error.kind())
                .expect("executor creation");
            executor.ok().expect("validated executor").run(|executor| {
                executor
                    .enter_with(
                        |mut sess| {
                            sess.run_quiesce(&mut finish, Empty {}, TIMEOUT)
                                .expect("application teardown")
                                .expect("application drive");
                        },
                        recovery::Policy::new(|event| reports.push(event), common::recover),
                    )
                    .as_ref()
                    .map_err(|error| error.cause())
                    .expect("executor completion");
            });
            assert!(
                reports.is_empty(),
                "unexpected reclaimed operations: {reports:?}"
            );
            Ok::<_, Infallible>(())
        })
    })
    .with(|| main.run(TIMEOUT, || ()))
    .expect("runtime shutdown worker");

    assert!(ready.is_ok(), "runtime readiness");
    assert!(stopped.is_ok(), "runtime shutdown");
    assert!(matches!(outcome, lifecycle::Outcome::Done(())));
}

#[test]
fn cancelled_ready() {
    let (main, worker) = lifecycle::Main::try_pair().expect("ready shutdown pair");
    assert!(matches!(
        main.wait_ready(time::Duration::ZERO),
        Err(lifecycle::WaitError::Timeout { .. })
    ));
    let main = main.stop().expect("pre-install shutdown");
    let outcome = worker.run(|source| {
        source
            .control()
            .wait_stop_until(time::Instant::now())
            .expect("queued shutdown");
        let mut finish = Completion::new();
        let mut create = Attempt::new();
        let mut reports = Vec::new();

        let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("executor creation");
        executor.ok().expect("validated executor").run(|executor| {
            executor
                .enter_with(
                    |mut sess| {
                        sess.run_quiesce(&mut finish, Empty {}, TIMEOUT)
                            .expect("application teardown")
                            .expect("application drive");
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                )
                .as_ref()
                .map_err(|error| error.cause())
                .expect("executor completion");
        });
        assert!(
            reports.is_empty(),
            "unexpected reclaimed operations: {reports:?}"
        );
        Ok::<_, Infallible>(())
    });
    assert!(matches!(outcome, lifecycle::Outcome::Done(())));
    main.wait_ready(time::Duration::MAX)
        .expect("late installation");
    main.wait_done(time::Duration::ZERO)
        .expect("completed worker");
}

#[test]
fn drop_finishes_once() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let stops = Rc::new(Cell::new(Some(0)));
    let ends = Rc::new(Cell::new(Some(0)));
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    let clock = sess.driver().driver_ref().clock();
                    let before = clock.now();
                    let result = sess
                        .with_app(
                            &mut finish,
                            dispatch::Builder::new(Count {
                                stops: Rc::clone(&stops),
                                ends: Rc::clone(&ends),
                                root: None,
                            })
                            .probe::<KeyTag<0>>(),
                            |_| {
                                assert!(clock.now() >= before);
                                Err::<(), _>(19u8)
                            },
                        )
                        .expect("application cleanup");
                    assert_eq!(result, Err(19));
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(stops.get(), Some(1));
    assert_eq!(ends.get(), Some(1));
}

#[test]
fn shutdown_ignores_timers() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let stops = Rc::new(Cell::new(Some(0)));
    let ends = Rc::new(Cell::new(Some(0)));
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    let driver = sess.driver().driver_ref();
                    let target = driver
                        .targets::<route::KeyTag<0>>()
                        .bind(SlotIndex::ZERO, route::Epoch::INITIAL)
                        .dispatch();
                    let ready = (driver.ready().make_slot(target)).expect("ready slot");
                    let access = sess.driver();
                    let clock = driver.clock();
                    let now = clock.now();
                    let limit = now.clamp_add(time::Duration::from_secs(60));
                    let timer = access.driver_ref().timer();
                    let alarm = (timer.try_alarm()).expect("timer alarm");
                    alarm.arm(limit, Waker::from(ready.target()));
                    let alarm = pin::pin!(alarm);
                    assert_eq!(
                        alarm.as_ref().poll(now, Waker::from(ready.target()),),
                        task::Poll::Pending,
                    );

                    assert!(
                        sess.with_quiesce(
                            &mut finish,
                            dispatch::Builder::new(Count {
                                stops: Rc::clone(&stops),
                                ends: Rc::clone(&ends),
                                root: None
                            })
                            .probe::<KeyTag<0>>(),
                            TIMEOUT,
                            |_| assert!(clock.now() >= now)
                        )
                        .is_ok(),
                        "application teardown must ignore unrelated timers"
                    );

                    assert!(alarm.is_armed());
                    alarm.clear();
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert_eq!(stops.get(), Some(1));
    assert_eq!(ends.get(), Some(1));
}

fn drain(config: Positive, count: usize, outcome: drain::Outcome, deadline: bool) {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let begun = Rc::new(Cell::new(false));
    let forced = Rc::new(Cell::new(false));
    let pair = (shutdown::Pair::new()).expect("shutdown pair");
    let (source, trig) = pair.split();
    let shutdown = if outcome == drain::Outcome::Quiesced {
        Some((trig, config.get() * 2))
    } else {
        assert!(trig.fire().is_ok());
        None
    };
    let mut reports = Vec::new();

    let executor = Configured::quic::<2, 8>().try_source(&mut create, source);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut sess| {
                    let deadline =
                        deadline.then(|| sess.driver().driver_ref().clock().now().into());
                    let result = sess.with_quiesce(
                        &mut finish,
                        DrainProbe {
                            begun: Rc::clone(&begun),
                            forced: Rc::clone(&forced),
                            remaining: count,
                            deadline,
                            shutdown,
                            fired: |fired: Result<(), shutdown::FireError>| {
                                fired.expect("shutdown after serving")
                            },
                        },
                        TIMEOUT,
                        |app| app.run_drain(config),
                    );
                    assert!(result.is_ok(), "application drain and finalization");
                    let Some(result) = result.ok() else { return };
                    assert!(result.is_ok(), "application drain and finalization");
                    let Some(report) = result.ok() else { return };
                    assert!(report.outcome() == outcome, "unexpected drain outcome");
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );

    assert!(begun.get());
    assert_eq!(forced.get(), outcome == drain::Outcome::Forced);
}
