pub mod common;

use std::{cell::Cell, convert::Infallible, rc::Rc, time, time::Duration};

use dope_core::driver::life::{creation::Attempt, recovery};
use dope_manifold::timing::interval::Interval;
use dope_runtime::executor::session::Completion;
use dope_test::{
    fibers,
    harness::signals::{Gate, Progress as _},
};

use crate::common::{
    manifold::root::{Host, Root},
    scenario::Runtime,
};

#[test]
fn reuses_route_after_ticks() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    {
        let result = Runtime::throughput().try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    }
    .run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let armed =
                    (Interval::every_second(&mut session.driver())).expect("interval ready slot");
                let result = session.with_quiesce(
                    &mut finish,
                    Host::new(armed, Gate::new()),
                    Duration::from_secs(2),
                    |mut app| {
                        fibers::TEST
                            .try_pause(&mut app, time::Duration::from_millis(20))
                            .expect("observation pause");
                    },
                );
                assert!(result.is_ok(), "armed application teardown");

                let gate = Gate::new();
                let fired =
                    (Interval::every_second(&mut session.driver())).expect("interval ready slot");
                let result = session.with_quiesce(
                    &mut finish,
                    Host::new(fired, gate.clone()),
                    Duration::from_secs(2),
                    |mut app| {
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &gate, 1)
                                .expect("progress wait"),
                            fibers::Wait::Reached
                        )
                    },
                );
                assert!(result.is_ok(), "ticked application teardown");

                let gate = Gate::new();
                let reused =
                    (Interval::every_second(&mut session.driver())).expect("interval ready slot");
                let result = session.with_quiesce(
                    &mut finish,
                    Host::new(reused, gate.clone()),
                    Duration::from_secs(2),
                    |mut app| {
                        fibers::TEST
                            .try_pause(&mut app, time::Duration::from_millis(20))
                            .expect("observation pause");
                        assert_eq!(gate.hits(), 0, "stale tick crossed app scopes");
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &gate, 1)
                                .expect("progress wait"),
                            fibers::Wait::Reached
                        );
                    },
                );
                assert!(result.is_ok(), "reused application teardown");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        result
            .as_ref()
            .map_err(|error| error.cause())
            .expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn observes_manual_time() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    {
        let result = Runtime::throughput().try_executor(&mut create);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    }
    .run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let start = (session
                    .driver()
                    .driver_ref()
                    .clock()
                    .now()
                    .try_add(time::Duration::from_millis(1)))
                .expect("manual start must fit the clock");
                let observed = Rc::new(Cell::new(None));
                let interval =
                    (Interval::every_second(&mut session.driver())).expect("interval ready slot");
                let app = Host::recording(interval, Gate::new(), observed.clone());
                let result = session.with_at_for(
                    &mut finish,
                    app,
                    start,
                    Duration::from_secs(2),
                    |mut app| {
                        let now = (start.try_add(time::Duration::from_secs(17)))
                            .expect("manual turn must fit the clock");
                        let polls = Cell::new(0);
                        let fixed = app
                            .drive(now, Root(&polls))
                            .expect("manual coordinate turn");
                        assert_eq!(polls.get(), 1, "root polled once");
                        assert!(fixed.now() == now);
                        assert_eq!(observed.get(), Some(now.into_raw()));
                    },
                );
                assert!(result.is_ok(), "manual application teardown");
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        result
            .as_ref()
            .map_err(|error| error.cause())
            .expect("native retirement");
        result.ok().expect("native retirement")
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
