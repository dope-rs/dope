use std::convert::Infallible;

use dope_core::driver::{
    life::{creation, recovery},
    route::KeyTag,
};

use crate::common::{driver::support::Targets as _, wake::Ready as _};
pub mod common;

use std::time::{Duration, Instant};

use dope_core::driver::{
    life::domain,
    route::{Epoch, indices::SlotIndex},
    schedule,
    schedule::{clock, clock::timer, ready::wake::Waker},
    settings,
    settings::demands::Schedule,
};
use settings::demands::slots::Slots;

use crate::common::{driver::support::ROUTE, rt::Runtime};

fn nofile_soft_limit() -> libc::rlim_t {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` names writable storage for one resource limit.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit.rlim_cur
}

#[test]
fn preserves_nofile_limit() {
    let before = nofile_soft_limit();
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(
        Runtime::quic::<2, 8>()
            .config()
            .with_domains(settings::Domain::ONE),
    );
    assert!(driver.is_ok(), "driver creation");
    let after = nofile_soft_limit();
    assert_eq!(after, before);
}

#[test]
fn outlives_scope_drop() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let config = config.set_schedule(settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        config.schedule().ready(),
        Slots::fixed::<3>(),
    )));
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    assert!(driver.is_ok(), "driver creation");
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |scope, _, _| {
                    let timer = {
                        let mut scope = scope;
                        scope.context().driver_ref().timer()
                    };
                    let alarms = [(); 3].map(|()| timer.try_alarm());
                    assert!(alarms.iter().all(Result::is_ok));
                    assert!(matches!(timer.try_alarm(), Err(timer::Exhausted::Slots)));
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn due_work_spends_budget() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let config = config.set_schedule(settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        config.schedule().ready(),
        Slots::fixed::<{ 2 * schedule::LIMIT as u32 }>(),
    )));
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    assert!(driver.is_ok(), "driver creation");
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.with_driver(|context, mut turn| {
                        let timer = context.driver_ref().timer();
                        let driver = context.driver_ref();
                        let slot = (driver.ready().make_slot(driver.target())).expect("ready slot");
                        let now = Instant::now();
                        let moment = {
                            let sampled = turn.begin_at(clock::Sample::new(now));
                            sampled.turn().clock().now()
                        };
                        let alarms = ((0..2 * schedule::LIMIT)
                            .map(|_| timer.try_alarm())
                            .collect::<Result<Vec<_>, _>>())
                        .expect("timer capacity");
                        let early = moment.try_add(Duration::from_secs(1));
                        assert!(early.is_ok(), "first deadline range");
                        let early = early.expect("first deadline");
                        let late = moment.try_add(Duration::from_secs(2));
                        assert!(late.is_ok(), "second deadline range");
                        let late = late.expect("second deadline");
                        for (index, alarm) in alarms.iter().enumerate() {
                            let due = if index < schedule::LIMIT { early } else { late };
                            alarm.arm(due, Waker::from(slot.target()));
                        }

                        {
                            turn.sample(clock::Sample::new(now));
                            let mut active = turn.begin();
                            active.expire();
                            assert!(!active.is_exhausted());
                        }

                        {
                            turn.sample(clock::Sample::new(now + Duration::from_secs(3)));
                            let mut active = turn.begin();
                            active.expire();
                            assert!(active.is_exhausted());
                        }

                        {
                            turn.sample(clock::Sample::new(now + Duration::from_secs(3)));
                            let mut active = turn.begin();
                            active.expire();
                            assert!(active.is_exhausted());
                        }
                        let mut active = turn.begin();
                        active.expire();
                        assert!(!active.is_exhausted());
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn zero_capacity_rejects() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let config = config.set_schedule(settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        config.schedule().ready(),
        Slots::fixed::<0>(),
    )));
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    assert!(driver.is_ok(), "driver creation");
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    let timer = scope.context().driver_ref().timer();
                    assert!(matches!(timer.try_alarm(), Err(timer::Exhausted::Slots)));
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn lease_gates_slot_reuse() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let config = config.set_schedule(settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        config.schedule().ready(),
        Slots::fixed::<1>(),
    )));
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    assert!(driver.is_ok(), "driver creation");
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.with_driver(|context, mut turn| {
                        let timer = context.driver_ref().timer();
                        let driver = context.driver_ref();
                        let ready =
                            (driver.ready().make_slot(driver.target())).expect("ready slot");
                        let now = Instant::now();
                        let moment = {
                            let sampled = turn.begin_at(clock::Sample::new(now));
                            sampled.turn().clock().now()
                        };

                        {
                            let idle = (timer.try_alarm()).expect("idle timer slot");
                            assert!(!idle.is_armed());
                            assert!(matches!(timer.try_alarm(), Err(timer::Exhausted::Slots)));
                        }

                        {
                            let armed = (timer.try_alarm()).expect("armed timer slot");
                            let due = moment.try_add(Duration::from_secs(1));
                            assert!(due.is_ok(), "armed deadline range");
                            let due = due.expect("armed deadline");
                            armed.arm(due, Waker::from(ready.target()));
                            assert!(armed.is_armed());
                            assert!(matches!(timer.try_alarm(), Err(timer::Exhausted::Slots)));
                            assert!(
                                armed
                                    .arm_and_poll(
                                        moment.into(),
                                        moment,
                                        Waker::from(ready.target())
                                    )
                                    .is_ready()
                            );
                            assert!(!armed.is_armed());
                            assert!(matches!(timer.try_alarm(), Err(timer::Exhausted::Slots)));
                        }

                        {
                            let fired = (timer.try_alarm()).expect("fired timer slot");
                            fired.arm(moment.into(), Waker::from(ready.target()));
                            {
                                turn.sample(clock::Sample::new(now));
                                let mut active = turn.begin();
                                active.expire();
                                assert!(!active.is_exhausted());
                            }
                            assert!(matches!(timer.try_alarm(), Err(timer::Exhausted::Slots)));
                            assert!(fired.is_armed());
                        }

                        let reused = (timer.try_alarm()).expect("released timer slot");
                        assert!(!reused.is_armed());
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn rearm_replaces_delivery() {
    let config = Runtime::quic::<2, 8>()
        .config()
        .with_domains(settings::Domain::ONE);
    let config = config.set_schedule(settings::Schedule::fixed::<0, 0>().resolve(Schedule::new(
        config.schedule().ready(),
        Slots::fixed::<1>(),
    )));
    let mut creation = creation::Attempt::new();
    let driver = creation.try_driver(config);
    assert!(driver.is_ok(), "driver creation");
    let driver = driver
        .map_err(|error| error.kind())
        .expect("driver creation");
    let mut reports = Vec::new();

    assert!(
        domain::Domain::new(driver)
            .start()
            .expect("fresh generation")
            .enter_with(
                (),
                |mut scope, _, _| {
                    scope.with_driver(|context, mut turn| {
                        let timer = context.driver_ref().timer();
                        let driver = context.driver_ref();
                        let expected = [0_u16, 1].map(|slot| {
                            driver
                                .targets::<KeyTag<ROUTE>>()
                                .bind(SlotIndex::from(slot), Epoch::INITIAL)
                                .dispatch()
                        });
                        let [first, second] = expected
                            .map(|target| driver.ready().make_slot(target).expect("ready slots"));
                        let now = Instant::now();
                        let moment = {
                            let sampled = turn.begin_at(clock::Sample::new(now));
                            sampled.turn().clock().now()
                        };
                        let alarm = (timer.try_alarm()).expect("timer slot");
                        let early = moment.try_add(Duration::from_secs(1));
                        assert!(early.is_ok(), "first deadline range");
                        let early = early.expect("first deadline");
                        let late = moment.try_add(Duration::from_secs(2));
                        assert!(late.is_ok(), "second deadline range");
                        let late = late.expect("second deadline");
                        alarm.arm(early, Waker::from(first.target()));
                        alarm.arm(late, Waker::from(second.target()));

                        {
                            let mut active = turn
                                .begin_at(clock::Sample::new(now + Duration::from_millis(1500)));
                            active.expire();
                            assert_eq!(active.ready_round(|_| {}), 0);
                        }

                        let mut active =
                            turn.begin_at(clock::Sample::new(now + Duration::from_secs(3)));
                        let moment = active.turn().clock().now();
                        active.expire();
                        let mut fired = Vec::new();
                        assert_eq!(active.ready_round(|token| fired.push(token)), 1);
                        assert_eq!(fired.len(), 1);
                        assert!(expected[1].matches(fired[0]));
                        let next = moment
                            .try_add(Duration::from_secs(1))
                            .expect("rearmed deadline");
                        assert!(
                            alarm
                                .arm_and_poll(next, moment, Waker::from(first.target()))
                                .is_pending()
                        );
                        assert_eq!(timer.earliest(), Some(next));
                        drop(active);

                        {
                            let mut active = turn
                                .begin_at(clock::Sample::new(now + Duration::from_millis(3500)));
                            active.expire();
                            assert_eq!(active.ready_round(|_| {}), 0);
                        }

                        let mut active =
                            turn.begin_at(clock::Sample::new(now + Duration::from_secs(4)));
                        let moment = active.turn().clock().now();
                        active.expire();
                        fired.clear();
                        assert_eq!(active.ready_round(|token| fired.push(token)), 1);
                        assert!(expected[0].matches(fired[0]));
                        assert!(alarm.poll(moment, Waker::from(first.target())).is_ready());
                    });
                },
                recovery::Policy::new(|event| reports.push(event), recover)
            )
            .is_ok()
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}
