use std::{array, cell::Cell, convert::Infallible, rc::Rc};

use dope_test::harness::scenario::TIMEOUT;
use engine::{
    core::{
        driver::life::{
            creation::Attempt,
            recovery::{Policy, Recovery},
        },
        io::Reclaimed,
    },
    runtime::executor::{
        InstallError,
        session::{self, Completion},
    },
};

pub mod common;

use common::{
    macros::fairness::{
        AwaitWork, COORD_TARGET, COORD_WINDOWS, Child, Deferred, Eager, IDLE_WINDOW, Limited,
        Nested, Pair, Parent, PlanObserver, PlanOwner, Planned, Rounds, SPARSE_WINDOWS,
        native::{Count, Mixed},
        tree::{self, Absent, Branch, Head, Idle, Owned, Unselected},
    },
    native,
    owners::Guard,
};

#[test]
fn sparse_admitted_turns() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let active = Rc::new(Cell::new(false));
                let counts = array::from_fn::<_, 4, _>(|_| Rc::new(Cell::new(0)));
                let app = Nested {
                    first: Pair {
                        first: Eager::new(&active, &counts[0]),
                        second: Eager::new(&active, &counts[1]),
                    },
                    second: Pair {
                        first: Eager::new(&active, &counts[2]),
                        second: Eager::new(&active, &counts[3]),
                    },
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        let snapshot = app
                            .drive_for(
                                TIMEOUT,
                                Rounds {
                                    windows: &SPARSE_WINDOWS,
                                    active: &active,
                                    counts: &counts,
                                },
                            )
                            .expect("bounded sparse turns")
                            .expect("remaining application credit");
                        assert_eq!(snapshot, [2; 4]);
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn coordinated_progress() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let active = Rc::new(Cell::new(false));
                let counts = array::from_fn::<_, 2, _>(|_| Rc::new(Cell::new(0)));
                let app = Parent {
                    child: Child {
                        active: Rc::clone(&active),
                        count: Rc::clone(&counts[1]),
                    },
                    active: Rc::clone(&active),
                    count: Rc::clone(&counts[0]),
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        let snapshot = app
                            .drive_for(
                                TIMEOUT,
                                Rounds {
                                    windows: &COORD_WINDOWS,
                                    active: &active,
                                    counts: &counts,
                                },
                            )
                            .expect("bounded coordinator turns")
                            .expect("remaining application credit");
                        assert!(snapshot.into_iter().all(|count| count > 0), "{snapshot:?}");
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn retained_plan_drop() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let observed = Rc::new(PlanObserver::default());
                let app = Planned {
                    owner: PlanOwner {
                        observed: Rc::clone(&observed),
                    },
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        app.drive_for(
                            TIMEOUT,
                            Rounds {
                                windows: &COORD_WINDOWS,
                                active: &Cell::new(false),
                                counts: &[],
                            },
                        )
                        .expect("bounded retained-plan turns")
                        .expect("remaining application credit");
                        assert_eq!(observed.dropped.get(), None);
                    })
                    .expect("application teardown");
                assert!(observed.parks.get() >= COORD_WINDOWS.len());
                assert_eq!(observed.prepared.get(), 1);
                assert_eq!(observed.dropped.get(), Some(observed.parks.get()));
                assert!(
                    observed.finished_at_drop.get(),
                    "plan dropped before application finish"
                );
                assert!(!observed.repeated.get(), "plan dropped twice");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn refunded_coord_resumes() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let observed = Rc::new(Deferred::default());
                let app = Limited {
                    child: Child {
                        active: Rc::new(Cell::new(false)),
                        count: Rc::new(Cell::new(0)),
                    },
                    observed: Rc::clone(&observed),
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        let calls = app
                            .drive_for(TIMEOUT, AwaitWork { observed })
                            .expect("exhausted coordinator resumes without a root wake");
                        assert_eq!(calls, COORD_TARGET);
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn idle_tree() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let active = Rc::new(Cell::new(false));
                let counts = array::from_fn::<_, 130, _>(|_| Rc::new(Cell::new(0)));
                let nodes = array::from_fn(|index| Rc::clone(&counts[index]));
                let app = Branch {
                    first: Head {
                        tree: tree::new(&active, &nodes),
                        active: Rc::clone(&active),
                        count: Rc::clone(&counts[128]),
                    },
                    second: Idle {
                        active: Rc::clone(&active),
                        count: Rc::clone(&counts[129]),
                    },
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        let snapshot = app
                            .drive_for(
                                TIMEOUT,
                                Rounds {
                                    windows: &IDLE_WINDOW,
                                    active: &active,
                                    counts: &counts,
                                },
                            )
                            .expect("one idle coordinator pass")
                            .expect("remaining application credit");
                        assert_eq!(snapshot, [1; 130]);
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn absent_tree() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let active = Rc::new(Cell::new(false));
                let counts = array::from_fn::<_, 129, _>(|_| Rc::new(Cell::new(0)));
                let nodes = array::from_fn(|index| Rc::clone(&counts[index]));
                let app = Absent {
                    first: Head {
                        tree: tree::new(&active, &nodes),
                        active: Rc::clone(&active),
                        count: Rc::clone(&counts[128]),
                    },
                    second: None,
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        let snapshot = app
                            .drive_for(
                                TIMEOUT,
                                Rounds {
                                    windows: &IDLE_WINDOW,
                                    active: &active,
                                    counts: &counts,
                                },
                            )
                            .expect("one pass with an absent subtree")
                            .expect("remaining application credit");
                        assert_eq!(snapshot, [1; 129]);
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn reject_live_limit() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let drops = Cell::new(0);
                let entered = Cell::new(false);
                let active = Rc::new(Cell::new(false));
                let count = Rc::new(Cell::new(0));
                let nodes = array::from_fn(|_| Rc::clone(&count));
                let app = Owned {
                    app: Head {
                        tree: Head {
                            tree: Branch {
                                first: tree::new(&active, &nodes),
                                second: tree::new(&active, &nodes),
                            },
                            active: Rc::clone(&active),
                            count: Rc::clone(&count),
                        },
                        active,
                        count,
                    },
                    guard: Guard(&drops),
                };
                let result = session.with_quiesce(&mut report, app, TIMEOUT, |_| {
                    entered.set(true);
                });
                assert!(matches!(
                    result,
                    Err(session::Error::Install {
                        cause: InstallError::Coordinators {
                            count,
                            limit: 256,
                        },
                        disarm: None,
                    }) if count.get() == 258
                ));
                assert!(!entered.get());
                assert_eq!(drops.get(), 1);
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn unselected_tree() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let start = session.driver().driver_ref().clock().now().into();
                let active = Rc::new(Cell::new(false));
                let counts = array::from_fn::<_, 256, _>(|_| Rc::new(Cell::new(0)));
                let first = array::from_fn(|index| Rc::clone(&counts[index]));
                let second = array::from_fn(|index| Rc::clone(&counts[index + 128]));
                let app = Branch {
                    first: Branch {
                        first: tree::new(&active, &first),
                        second: tree::new(&active, &second),
                    },
                    second: Unselected,
                };
                session
                    .with_at_for(&mut report, app, start, TIMEOUT, |mut app| {
                        let fixed = app
                            .drive_for(
                                start,
                                TIMEOUT,
                                Rounds {
                                    windows: &IDLE_WINDOW,
                                    active: &active,
                                    counts: &counts,
                                },
                            )
                            .expect("idle selected coordinators reach a fixed point");
                        assert_eq!(
                            fixed.into_output().expect("remaining application credit"),
                            [1; 256]
                        );
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
    });
}

#[test]
fn native_coord_share() {
    let mut attempt = Attempt::new();
    let mut report = Completion::new();
    let executor = native::try_executor(&mut attempt);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("runtime creation");
    executor.ok().expect("created executor").run(|executor| {
        let result = executor.enter_with(
            |mut session| {
                let active = Rc::new(Cell::new(false));
                let counts = array::from_fn::<_, 2, _>(|_| Rc::new(Cell::new(0)));
                let app = Mixed {
                    first: Count::new(&active, &counts[0]),
                    child: Child {
                        active: Rc::clone(&active),
                        count: Rc::clone(&counts[1]),
                    },
                };
                session
                    .with_quiesce(&mut report, app, TIMEOUT, |mut app| {
                        let snapshot = app
                            .drive_for(
                                TIMEOUT,
                                Rounds {
                                    windows: &IDLE_WINDOW,
                                    active: &active,
                                    counts: &counts,
                                },
                            )
                            .expect("one mixed coordinator pass")
                            .expect("remaining application credit");
                        assert_eq!(snapshot, [128; 2]);
                    })
                    .expect("application teardown");
            },
            Policy::new(shutdown, recover),
        );
        assert!(result.is_ok(), "test runtime completed");
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
