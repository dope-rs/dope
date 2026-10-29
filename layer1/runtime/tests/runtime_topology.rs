use std::{cell::Cell, convert::Infallible};

use dope_core::driver::{life::recovery, route::KeyTag};
use dope_runtime::executor::session::Completion;
use dope_test::harness::scenario::TIMEOUT;
pub mod common;

use std::mem;

use dope_core::{
    driver::{
        route, settings,
        settings::{
            claim,
            demands::{Accept, Entries, Files, Outbound, Receive, Schedule},
            errors,
        },
        storage,
    },
    io::transfer,
};
use dope_runtime::topology::{self, error::Kind};
use dope_test::alloc::Tracker;
use settings::demands::slots;

use crate::common::runtime::topology::*;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn derives_prepared_layout() {
    let storage = Storage(
        claim::Claim::new(settings::Routes::singleton::<KeyTag<1>>()).files(Files::new(
            Accept::from(slots::Slots::fixed::<2>()),
            Outbound::fixed::<3>(),
        )),
    );
    let install = Install(
        claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>())
            .files(Files::new(
                Accept::from(slots::Slots::fixed::<4>()),
                Outbound::fixed::<1>(),
            ))
            .schedule(Schedule::fixed::<6, 7>()),
    );
    let prepared = (topology::Plan::new(storage, install).for_profile::<Profile>())
        .expect("distinct resource routes must prepare");
    let layout = prepared.layout();

    assert_eq!(layout.file_slots().accept().count().get(), 6);
    assert_eq!(layout.file_slots().outbound().count().get(), 4);
    assert!((layout.queue_layout()) == (settings::QueueLayout::fixed::<8, 8>()));
    assert_eq!(layout.schedule().ready().count().get(), 6);
    assert_eq!(layout.schedule().timers().count().get(), 7);
}

#[test]
fn resolves_without_alloc() {
    assert!((PROFILE_CONFIG.file_slots()) == (<Profile as settings::Profile>::FILES));
    let storage = Storage(claim::Claim::new(settings::Routes::singleton::<KeyTag<1>>()));
    let install = Install(claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>()));

    let (prepared, allocs) =
        Tracker::usage(|| topology::Plan::new(storage, install).for_profile::<Profile>());

    assert_eq!(allocs, (0, 0));
    assert!(prepared.is_ok(), "static resource tree is valid");
}

#[test]
fn rejects_duplicate_routes() {
    let storage = Storage(claim::Claim::new(settings::Routes::singleton::<KeyTag<9>>()));
    let install = Install(claim::Claim::new(settings::Routes::singleton::<KeyTag<9>>()));

    let error = topology::Plan::new(storage, install).for_profile::<Profile>();
    assert!(
        matches!(error, Err(errors::Claim::DuplicateRoute(actual)) if actual == route::Id::fixed::<9>())
    );
}

#[test]
fn uses_exact_receive_pool() {
    let storage = Storage(claim::Claim::new(settings::Routes::singleton::<KeyTag<1>>()));
    let install = Install(
        claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>()).receive(Receive::new(
            (Entries::try_from(3_usize)).expect("receive entry count"),
            transfer::Len::fixed::<17>(),
        )),
    );
    let prepared = (topology::Plan::new(storage, install).for_receive::<Profile>())
        .expect("distinct exact resource routes must prepare");

    assert!((prepared.layout().receive()) == (settings::Receive::fixed::<4, 17>()));
}

#[test]
fn keeps_outer_authority() {
    let mut finish = Completion::new();
    use storage::Value;
    let storage = 11;
    let install = 7;
    let prepared = (topology::Plan::new(Value::new(&storage), Borrowed(&install))
        .for_install::<Profile>())
    .expect("borrowed resource routes must prepare");
    let mut reports = Vec::new();

    prepared.enter_local(
        |scope: topology::Worker<'_, '_, '_, '_, _, _, _>| {
            let app = scope.install().expect("borrowed installation authority");
            app.with_quiesce(&mut finish, TIMEOUT, |_| {})
                .expect("finalize borrowed topology");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |entered| {
            entered
                .as_ref()
                .map_err(|error| error.kind())
                .expect("enter borrowed topology domain");
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn lends_application() {
    use std::rc::Rc;

    let mut complete = Completion::new();
    let value = 7;
    for reject in [false, true] {
        let called = Rc::new(Cell::new(false));
        let observed = Rc::clone(&called);
        let borrowed = &value;
        let prepared = topology::Plan::new(
            storage::Value::new(borrowed),
            Lending {
                value: borrowed,
                reject,
            },
        )
        .for_install::<Profile>()
        .expect("lending application layout");
        let (prepared, completion) = (prepared, &mut complete);
        prepared.with_local(
            |worker| {
                worker.lend(|setup, stage| {
                    setup.install(stage, |root, stage| {
                        topology::lending::Stage::with_app(stage, completion, root, |mut app| {
                            observed.set(true);
                            drop(observed);
                            Err::<(), _>((
                                app.inspect(|root| root, |root| *root.get_ref().value),
                                borrowed,
                            ))
                        })
                    })
                })
            },
            recovery::Policy::new(drop, recover),
            |outcome| {
                outcome
                    .as_ref()
                    .map_err(|error| error.kind())
                    .expect("enter lending topology domain");
                let Ok(outcome) = outcome else {
                    return;
                };
                if reject {
                    assert_eq!(outcome.expect_err("lending installer rejects"), value);
                } else {
                    let (actual, saved) = outcome
                        .expect("lending installer")
                        .expect("lending application completes")
                        .expect_err("application retains rejection");
                    assert_eq!((actual, saved), (value, borrowed));
                }
            },
        );
        assert_eq!(called.get(), !reject);
    }
}

#[test]
fn yields_before_publish() {
    let mut finish = Completion::new();
    let valid = Cell::new(true);
    let storage = Storage(claim::Claim::new(settings::Routes::singleton::<KeyTag<1>>()));
    let install = Install(claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>()));
    let prepared = (topology::Plan::new(storage, install).for_profile::<Profile>())
        .expect("distinct resource routes must prepare");
    let mut reports = Vec::new();

    prepared.try_scoped(
        |worker: topology::Worker<'_, '_, '_, '_, _, _, _>| {
            let app = (worker.install_with(|install, stage| {
                assert_eq!(mem::size_of_val(&install), mem::size_of::<Install>());
                assert_eq!(mem::size_of_val(stage.storage()), 0);
                YieldOnce(false, &valid)
            }))
            .expect("drive yielding install");
            app.with_quiesce(&mut finish, TIMEOUT, |_| {})
                .expect("finalize yielded application");
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |entered| {
            entered
                .as_ref()
                .map_err(|error| error.kind())
                .expect("enter yielding topology domain");
        },
    );
    assert!(valid.get(), "startup work, clock, cause and cancellation");
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn diagnostic_keeps_owner() {
    use std::{io::ErrorKind, rc::Rc};

    use dope_core::driver::life::domain::Rejected;

    let owner = Rc::new(String::from("retained operation"));
    let op = {
        let owner = Rc::clone(&owner);
        move || owner
    };
    let error: topology::EnterError<_, _, ()> = topology::EnterError::Storage(Rejected {
        cause: ErrorKind::TimedOut,
        operation: op,
    });

    let mapped = Cell::new(false);
    let (kind, allocs) = Tracker::usage(|| {
        error.kind().map(|output| {
            mapped.set(true);
            output
        })
    });
    assert_eq!(allocs, (0, 0));
    assert!(!mapped.get());
    assert!(matches!(kind, Kind::Storage(ErrorKind::TimedOut)));
    assert_eq!(Rc::strong_count(&owner), 2);

    let rejected = match error {
        topology::EnterError::Storage(rejected) => Some(rejected),
        _ => None,
    }
    .expect("storage failure retains its operation");
    assert_eq!(rejected.cause, ErrorKind::TimedOut);
    {
        let value = (rejected.operation)();
        assert!(Rc::ptr_eq(&owner, &value));
    }
    assert_eq!(Rc::strong_count(&owner), 1);
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
