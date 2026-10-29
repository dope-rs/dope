use std::{cell::Cell, convert::Infallible, pin, ptr};

use dope_core::driver::{life::recovery, route::KeyTag};
pub mod common;

use std::mem;

use dope_core::driver::{
    route, settings,
    settings::{claim, errors},
};
use dope_runtime::{
    executor::session::Completion,
    shutdown, topology,
    topology::{
        lending,
        ops::{Compose, Operation as _},
    },
};
use dope_test::alloc::Tracker;

use crate::common::runtime::topology::{
    Profile, Storage,
    chains::{Head, Tail},
    stages,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn pins_then_lends_chain() {
    let mut finish = Completion::new();
    let storage = Storage(claim::Claim::new(settings::Routes::singleton::<KeyTag<1>>()));
    let topology = (
        Head(claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>())),
        Tail(
            claim::Claim::new(settings::Routes::singleton::<KeyTag<3>>()),
            &mut finish,
        ),
    );
    let prepared = (topology::Plan::new(storage, topology).for_profile::<Profile>())
        .expect("distinct chain routes must prepare");
    let mut reports = Vec::new();

    prepared.try_local(
        |scope: topology::Worker<'_, '_, '_, '_, _, _, _>| Tracker::usage(|| Compose.run(scope)),
        recovery::Policy::new(|event| reports.push(event), recover),
        |entered| {
            entered
                .as_ref()
                .map_err(|error| error.kind())
                .expect("chain domain must enter");
            let Ok((sizes, allocs)) = entered else {
                return;
            };

            assert_eq!(
                sizes.expect("head installation"),
                (0, mem::size_of::<usize>(), 0, mem::size_of::<usize>())
            );
            assert_eq!(allocs, (0, 0));
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn rejects_duplicate_tail() {
    let mut finish = Completion::new();
    let storage = Storage(claim::Claim::new(settings::Routes::singleton::<KeyTag<1>>()));
    let topology = (
        Head(claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>())),
        Tail(
            claim::Claim::new(settings::Routes::singleton::<KeyTag<2>>()),
            &mut finish,
        ),
    );

    let error = topology::Plan::new(storage, topology).for_profile::<Profile>();
    assert!(
        matches!(error, Err(errors::Claim::DuplicateRoute(actual)) if actual == route::Id::fixed::<2>())
    );
}

#[test]
fn drains_after_client_drop() {
    for external in [false, true] {
        let events = Cell::new(0);
        let address = Cell::new(ptr::null());
        let mut complete = Completion::new();
        let mut reports = Vec::new();
        let prepared = topology::Plan::new(Storage(claim::Claim::new(settings::Routes::EMPTY)), ())
            .for_profile::<Profile>()
            .expect("empty composition");
        let (source, trigger) = shutdown::Pair::new().expect("shutdown source").split();
        trigger.fire().expect("request shutdown");
        prepared.try_with(
            source,
            |worker: topology::Worker<'_, '_, '_, '_, _, _, _>| {
                worker.lend(|(), stage| {
                    let owner = stages::Owner {
                        events: &events,
                        address: &address,
                    };
                    stage.hold(owner, |held, stage| {
                        let _backing = stages::Backing(&events);
                        if external {
                            let result = held.lend_scope(stage, &mut complete, |issued| {
                                let (client, permit) = issued.into_parts();
                                let state = pin::pin!(client);
                                permit.try_run(
                                    |_driver| {
                                        Ok::<_, Infallible>((
                                            stages::Borrowed(state.as_ref()),
                                            37_u8,
                                        ))
                                    },
                                    |value, app| (value, app.run()),
                                )
                            });
                            let (value, request) = result
                                .as_ref()
                                .map_err(|error| match error {
                                    lending::Error::Build(error) => match *error {},
                                    lending::Error::Run(error) => error.cause(),
                                })
                                .expect("callback state ends before final provider drain");
                            assert_eq!(*value, 37);
                            request.as_ref().expect("observed shutdown");
                        } else {
                            held.with_lent(stage, &mut complete, stages::Create(&events))
                                .expect("child destruction precedes final provider drain");
                        }
                    });
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |entered| {
                entered
                    .as_ref()
                    .map_err(|error| error.kind())
                    .expect("lifecycle domain");
            },
        );
        assert_eq!(events.get(), 0x123456789a);
        assert!(
            reports.is_empty(),
            "unexpected native recovery: {reports:?}"
        );
    }
}

#[test]
fn rejects_held_provider() {
    for issued in [false, true] {
        let events = Cell::new(0);
        let address = Cell::new(ptr::null());
        let mut complete = Completion::new();
        let mut reports = Vec::new();
        let prepared = topology::Plan::new(Storage(claim::Claim::new(settings::Routes::EMPTY)), ())
            .for_profile::<Profile>()
            .expect("empty composition");
        prepared.try_local(
            |worker: topology::Worker<'_, '_, '_, '_, _, _, _>| {
                worker.lend(|(), stage| {
                    let owner = stages::Owner {
                        events: &events,
                        address: &address,
                    };
                    let rejected = stage.hold(owner, |held, stage| {
                        let _backing = stages::Backing(&events);
                        if issued {
                            let result = held.lend_scope(stage, &mut complete, |issued| {
                                let (_client, permit) = issued.into_parts();
                                permit.try_run(
                                    |_driver| Err::<(stages::Borrowed<'_, '_, '_, '_>, ()), _>(37),
                                    |(), app| app.run(),
                                )
                            });
                            assert!(matches!(result, Err(lending::Error::Build(37))));
                        } else {
                            let next = move || drop(held);
                            drop(next);
                        }
                        Err::<(), _>(37)
                    });
                    assert_eq!(rejected, Err(37));
                });
            },
            recovery::Policy::new(|event| reports.push(event), recover),
            |entered| {
                entered
                    .as_ref()
                    .map_err(|error| error.kind())
                    .expect("uninstalled domain");
            },
        );
        assert_eq!(events.get(), if issued { 0x159a } else { 0x9a });
        assert_eq!(address.get().is_null(), !issued);
        assert!(reports.is_empty());
    }
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
