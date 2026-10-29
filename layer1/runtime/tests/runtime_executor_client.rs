use std::convert::Infallible;

use dope_core::driver::life::{creation::Attempt, recovery};
use dope_runtime::executor::session::{self, Completion};
use dope_test::harness::scenario::TIMEOUT;
pub mod common;

use std::{cell::RefCell, hint, mem, time::Duration};

use dope_core::driver::schedule::{self, clock};
use dope_runtime::{
    client::Bound,
    executor::{InstallError, drive},
};
use dope_test::alloc::Tracker;

use crate::common::{runtime::executor::client::*, scenario::rt::Configured};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn immediate_baseline() {
    immediate(0);
}

#[test]
fn immediate_no_alloc() {
    immediate(ITERATIONS);
}

#[test]
fn manual_no_alloc() {
    let mut polls = Some(0);
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let start = (session
                        .driver()
                        .driver_ref()
                        .clock()
                        .now()
                        .try_add(Duration::from_millis(1)))
                    .expect("manual start deadline");
                    assert!(
                        session
                            .with_at_for(
                                &mut finish,
                                App { value: 0 },
                                start,
                                TIMEOUT,
                                |mut app| {
                                    let warm = app
                                        .drive_for(start, TIMEOUT, Immediate(0, &mut polls))
                                        .expect("warm manual drive");
                                    assert!(clock::Deadline::from(warm.now()) == start);
                                    assert_eq!(warm.into_output(), 0);
                                    let usage = dope_test::alloc::Tracker::during(|| {
                                        for output in 0..ITERATIONS {
                                            let fixed = app
                                                .drive_for(
                                                    start,
                                                    TIMEOUT,
                                                    Immediate(output, &mut polls),
                                                )
                                                .expect("manual drive");
                                            assert_eq!(
                                                hint::black_box(fixed.into_output()),
                                                output
                                            );
                                        }
                                    });
                                    assert_eq!(usage, (0, 0));
                                }
                            )
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(polls, Some(ITERATIONS + 1));
}

#[test]
fn manual_clock_order() {
    let mut polls = Some(0);
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let start = (session
                        .driver()
                        .driver_ref()
                        .clock()
                        .now()
                        .try_add(Duration::from_millis(1)))
                    .expect("manual start deadline");
                    assert!(
                        session
                            .with_at_for(
                                &mut finish,
                                App { value: 0 },
                                start,
                                TIMEOUT,
                                |mut app| {
                                    let later = (start.try_add(Duration::from_secs(7)))
                                        .expect("later deadline");
                                    let fixed = app
                                        .drive_for(later, TIMEOUT, Immediate(13, &mut polls))
                                        .expect("later logical turn");
                                    assert!(clock::Deadline::from(fixed.now()) == later);
                                    assert_eq!(fixed.output(), &13);

                                    let error = match app.drive_for(
                                        start,
                                        TIMEOUT,
                                        Immediate(21, &mut polls),
                                    ) {
                                        Err(drive::Error::ClockMovedBackwards(error)) => {
                                            Some(error)
                                        }
                                        _ => None,
                                    }
                                    .expect(
                                        "logical time regression must retain its exact failure",
                                    );
                                    assert_eq!(
                                        error.current().checked_duration_since(error.requested()),
                                        Some(Duration::from_secs(7))
                                    );
                                }
                            )
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(polls, Some(1));
}

#[test]
fn rejects_wall_regression() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let future = session
                        .driver()
                        .driver_ref()
                        .clock()
                        .now()
                        .try_add(Duration::from_secs(3600))
                        .expect("manual future");
                    assert!(
                        session
                            .with_at_for(&mut finish, App { value: 0 }, future, TIMEOUT, |_| {})
                            .is_ok()
                    );

                    let mut called = false;
                    assert!(matches!(
                        session.with_app(&mut finish, App { value: 0 }, |_| called = true),
                        Err(session::Error::Drive(drive::Error::ClockMovedBackwards(_)))
                    ));
                    assert!(!called);
                    assert!(
                        session
                            .with_at_for(&mut finish, App { value: 0 }, future, TIMEOUT, |_| {})
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reject_keeps_clock() {
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    let future = session
                        .driver()
                        .driver_ref()
                        .clock()
                        .now()
                        .try_add(Duration::from_secs(3600))
                        .expect("manual future");
                    let events = RefCell::new(Vec::new());
                    let mut called = false;
                    assert!(matches!(
                        session.with_at_for(
                            &mut finish,
                            Rejected::<true>(&events),
                            future,
                            TIMEOUT,
                            |_| called = true,
                        ),
                        Err(session::Error::Install {
                            cause: InstallError::Coordinators { count, limit },
                            disarm: None,
                        })
                            if count.get() == schedule::LIMIT + 1 && limit == schedule::LIMIT
                    ));
                    let lifecycle = [
                        Lifecycle::Installed,
                        Lifecycle::Stopped,
                        Lifecycle::Finished,
                        Lifecycle::Dropped,
                    ];
                    assert_eq!(events.borrow().as_slice(), lifecycle);
                    events.borrow_mut().clear();
                    assert!(matches!(
                        session.with_at_for(
                            &mut finish,
                            Rejected::<false>(&events),
                            future,
                            TIMEOUT,
                            |_| called = true,
                        ),
                        Err(session::Error::Install {
                            cause: InstallError::Claim(
                                dope_core::driver::settings::errors::Claim::DuplicateRoute(id)
                            ),
                            disarm: None,
                        }) if id == dope_core::driver::route::Id::fixed::<3>()
                    ));
                    assert_eq!(events.borrow().as_slice(), lifecycle);
                    assert!(matches!(
                        session.with_app(&mut finish, App { value: 0 }, |_| called = true),
                        Err(session::Error::Drive(drive::Error::ClockMovedBackwards(error)))
                            if error.current() == future.into_raw()
                    ));
                    assert!(!called);
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn issued_is_owned() {
    let mut polls = Some(0);
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    assert!(
                        session
                            .with_quiesce(&mut finish, App { value: 7 }, TIMEOUT, |mut app| {
                                let client = app.root_client();
                                assert!(matches!(app.drive(Immediate(7, &mut polls)), Ok(7)));
                                assert!(matches!(app.drive(Immediate(11, &mut polls)), Ok(11)));
                                let (lease, value) = client.parts();
                                assert_eq!(mem::size_of_val(lease), 0);
                                assert_eq!(value, 7);
                            })
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(polls, Some(2));
}

#[test]
fn issues_client() {
    let mut polls = Some(0);
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    assert!(
                        session
                            .with_quiesce(
                                &mut finish,
                                Owner {
                                    source: App { value: 29 }
                                },
                                TIMEOUT,
                                |mut app| {
                                    assert!(matches!(app.drive(Immediate(0, &mut polls)), Ok(0)));
                                    let usage = dope_test::alloc::Tracker::during(|| {
                                        let client = app.root_client();
                                        assert!(matches!(
                                            app.drive(Immediate(31, &mut polls)),
                                            Ok(31)
                                        ));
                                        assert_eq!(client.parts().1, 29);
                                    });
                                    assert_eq!(usage, (0, 0));
                                }
                            )
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(polls, Some(2));
}

#[test]
fn custom_issue_is_owned() {
    let mut polls = Some(0);
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    assert!(
                        session
                            .with_quiesce(&mut finish, App { value: 13 }, TIMEOUT, |mut app| {
                                let client = app.client_with(
                                    |app| app,
                                    |context| {
                                        let (app, scope) = context.into_parts();
                                        Client {
                                            lease: scope,
                                            value: app.get_ref().value,
                                        }
                                    },
                                );
                                assert!(matches!(app.drive(Immediate(13, &mut polls)), Ok(13)));
                                assert_eq!(client.parts().1, 13);
                            })
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(polls, Some(1));
}

#[test]
fn bound_is_copy() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<Bound<'static, 'static, App>>();
}

fn immediate(extra: usize) {
    let mut polls = Some(0);
    let mut finish = Completion::new();
    let mut create = Attempt::new();
    let mut reports = Vec::new();

    let executor = Configured::throughput().try_executor(&mut create);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    executor.ok().expect("validated executor").run(|executor| {
        executor
            .enter_with(
                |mut session| {
                    assert!(
                        session
                            .with_quiesce(&mut finish, App { value: 0 }, TIMEOUT, |mut app| {
                                assert!(matches!(app.drive(Immediate(0, &mut polls)), Ok(0)));
                                let usage = Tracker::during(|| {
                                    for output in 0..extra {
                                        let actual = app
                                            .drive(Immediate(output, &mut polls))
                                            .expect("immediate drive");
                                        assert_eq!(hint::black_box(actual), output);
                                    }
                                });
                                assert_eq!(usage, (0, 0));
                            })
                            .is_ok()
                    );
                },
                recovery::Policy::new(|event| reports.push(event), recover),
            )
            .as_ref()
            .map_err(|error| error.cause())
            .expect("executor completion");
    });
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
    assert_eq!(polls, Some(extra + 1));
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
