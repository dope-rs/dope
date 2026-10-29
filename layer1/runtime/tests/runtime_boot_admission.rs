use std::convert::Infallible;

use dope_core::{
    Errno,
    driver::{
        life::{creation::Attempt, recovery},
        retained::flight,
        route::{Epoch, KeyTag, Tag as _, indices::SlotIndex},
        settings::{self, Profile as _},
        storage,
    },
    io::{self, fd::outbound::Reserved, socket},
};
use dope_runtime::{
    executor::{Executor, Op as _, drive},
    topology::{InstallError, Worker},
};
use dope_test::reclaim::Reports;
use socket::Prepared as _;

pub mod common;

use common::runtime::topology::{Admission, Profile};

type Route = KeyTag<7>;

#[test]
fn native_rejects_boot() {
    let counts = Admission::default();
    let mut attempt = Attempt::new();
    let config = settings::Config::for_profile::<Profile>();
    let mut reports = Reports::new(config, [Route::ROUTE]);
    let created = Executor::new(&mut attempt, config);
    assert!(
        created.is_ok(),
        "executor: {:?}",
        created.as_ref().err().map(|error| error.kind())
    );
    let Some(executor) = created.ok() else {
        return;
    };
    executor.with_storage(&counts).run(|exec| {
        let result = exec.enter_with(
            |mut scope| {
                let (slots, target, creation) = {
                    let mut driver = scope.driver();
                    let flights = driver
                        .storage
                        .reborrow()
                        .try_flights(flight::Need::<Route>::new(flight::Capacity::fixed::<1>()))
                        .expect("flight storage")
                        .slots();
                    let slots = Reserved::<Route>::try_new(&driver, Profile::FILES.outbound())
                        .expect("outbound slot");
                    let target = driver
                        .driver_ref()
                        .targets::<Route>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL);
                    let stream = socket::Inet::from_std(([127, 0, 0, 1], 9).into()).stream_spec();
                    let creation = socket::Open::try_new(
                        &mut driver,
                        &flights,
                        slots.bind(target).expect("socket target"),
                        stream,
                    )
                    .expect("socket submission")
                    .commit();
                    (slots, target, creation)
                };
                let rejected = (
                    (),
                    |worker: Worker<'_, '_, '_, '_, storage::Value<&Admission>, (), ()>| {
                        matches!(
                            worker.install_with(|_, boot| *boot.storage()),
                            Err(InstallError::Drive(drive::Error::Boot(_)))
                        )
                    },
                )
                    .run(scope);
                assert!(rejected, "startup must reject the existing native flight");
                assert!(
                    slots.bind(target).is_none(),
                    "original owner keeps its slot"
                );
                drop(creation);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "native owner is reclaimed by its driver");
    });
    assert_eq!((counts.polls.get(), counts.cancelled.get()), (0, 1));
    let events = reports.events().expect("native retirement succeeds");
    let closed = reports.closed().expect("descriptor retirement succeeds");
    assert!(
        matches!(events, [io::Reclaimed::Socket { slot, result, close, .. }]
        if slot.route() == Route::ROUTE
                && match (result, close, closed) {
                    (Ok(()), None, [retired]) => retired == slot,
                    (Ok(()), None | Some(Ok(())), []) => true,
                    (Err(error), None, []) => error.errno() == Errno::CANCELED
                        && error.step() == socket::CreationStep::Socket,
                _ => false,
            }),
        "the original socket reports its exact completion and close: {events:?}, {closed:?}"
    );
}

#[test]
fn capacity_cancels_boot() {
    let counts = Admission::default();
    let mut attempt = Attempt::new();
    let config = settings::Config::for_profile::<Profile>();
    let mut reports = Vec::new();
    let created = Executor::new(&mut attempt, config);
    assert!(
        created.is_ok(),
        "executor: {:?}",
        created.as_ref().err().map(|error| error.kind())
    );
    let Some(executor) = created.ok() else {
        return;
    };
    executor.with_storage(&counts).run(|exec| {
        let result = exec.enter_with(
            |mut scope| {
                let driver = scope.driver().driver_ref();
                let target = driver
                    .targets::<Route>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch();
                let ready = driver.ready().make_slot(target).expect("only ready slot");
                let rejected = (
                    (),
                    |worker: Worker<'_, '_, '_, '_, storage::Value<&Admission>, (), ()>| {
                        matches!(
                            worker.install_with(|_, boot| *boot.storage()),
                            Err(InstallError::Drive(drive::Error::Capacity(error)))
                                if error.available() == 0
                        )
                    },
                )
                    .run(scope);
                assert!(rejected, "startup must preserve exact capacity failure");
                drop(ready);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        );
        assert!(result.is_ok(), "driver retires after rejected startup");
    });
    assert_eq!((counts.polls.get(), counts.cancelled.get()), (0, 1));
    assert!(reports.is_empty(), "unexpected native work: {reports:?}");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
