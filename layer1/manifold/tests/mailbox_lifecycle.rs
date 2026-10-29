pub mod common;

use o3::depot::arena::slab::Vacancy;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

use std::{cell::Cell, convert::Infallible, mem, ptr, rc::Rc};

use dope_core::driver::{
    life::{domain, recovery},
    settings::claim::Needs,
};
use dope_manifold::{
    app::creation,
    client::attempt::{Completion, queue},
    timing::Throughput,
};
use dope_net::tcp;
use dope_runtime::executor::Executor;
use dope_test::{
    alloc::Tracker,
    scope::{self, Driver},
};
use o3::depot::capacity::Count;
use queue::mailbox::{Cancellation, Conflict, Domain, Error};

use crate::common::scenario::{
    Runtime,
    case::attempt::Mailbox,
    ports::{self, Counted as _, Loopback},
};

type Dropped<'a> = ports::Dropped<Box<dyn FnOnce() + 'a>>;

#[test]
fn pending_drop_pins() {
    let mut reports = Vec::new();

    Driver::new(Runtime::throughput().config()).with_scope(
        |scope| {
            let mailbox = Rc::new(
                (Domain::<tcp::Tcp, Dropped<'_>, u8>::try_new(
                    Count::new(1),
                    &mut scope.context().storage,
                ))
                .expect("mailbox source"),
            );
            let drops = Rc::new(Cell::new(0));
            let done = Rc::new(Cell::new(ptr::null::<()>()));
            let context = {
                let mailbox = Rc::downgrade(&mailbox);
                let drops = drops.clone();
                let done = done.clone();
                Dropped::new(Box::new(move || {
                    let mailbox = mailbox.upgrade().expect("source owns the dropping context");
                    assert!(!done.get().is_null(), "published completion receipt");
                    // SAFETY: the exact receipt is installed after start and remains
                    // in its original stack slot until this callback returns from drop.
                    let receipt = unsafe { &*done.get().cast::<Completion<'_, '_>>() };
                    assert!(matches!(
                        mailbox.try_settle(receipt, 7),
                        Err((Error::Slot(Conflict::Vacant(key)), 7)) if key == receipt.id()
                    ));
                    assert!(matches!(
                        mailbox.try_reserve(),
                        Err(Error::Queue(Vacancy::Full))
                    ));
                    drops.set(drops.get() + 1);
                }))
            };
            let receipt;
            {
                let lease = (mailbox.try_reserve())
                    .expect("pending attempt")
                    .start(context, Loopback(0).target());
                receipt = lease.receipt();
                done.set(ptr::from_ref(&receipt).cast());
            }
            done.set(ptr::null());
            assert_eq!(drops.get(), 1);
            assert!(matches!(
                mailbox.try_settle(&receipt, 7),
                Err((Error::Slot(Conflict::Vacant(key)), 7)) if key == receipt.id()
            ));
            assert!(mailbox.try_reserve().is_ok());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        scoped,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn rejected_keeps_owner() {
    let mut reports = Vec::new();

    Driver::new(Runtime::throughput().config()).with_scope(
        |scope| {
            let mailbox = Rc::new(
                (Domain::<tcp::Tcp, (), Dropped<'_>>::try_new(
                    Count::new(1),
                    &mut scope.context().storage,
                ))
                .expect("mailbox source"),
            );
            let lease = (mailbox.try_reserve())
                .expect("pending attempt")
                .start((), Loopback(0).target());
            let receipt = lease.receipt();
            let previous = Rc::new(Cell::new(0));
            assert!(mailbox.try_settle(&receipt, previous.counted()).is_ok());
            let drops = Rc::new(Cell::new(0));
            let rejected = {
                let mailbox = Rc::downgrade(&mailbox);
                let drops = drops.clone();
                let previous = previous.clone();
                let done = ptr::from_ref(&receipt).cast::<()>();
                Dropped::new(Box::new(move || {
                    let mailbox = mailbox.upgrade().expect("source owns the settled outcome");
                    // SAFETY: the caller retains this exact receipt in its original
                    // stack slot until after the rejected outcome has been dropped.
                    let receipt = unsafe { &*done.cast::<Completion<'_, '_>>() };
                    let (cause, _) = mailbox
                        .try_settle(receipt, Dropped::empty())
                        .err()
                        .expect("drop observes the restored settled payload");
                    assert!(matches!(
                        cause,
                        Error::Slot(Conflict::Settled { key, current })
                            if key == receipt.id() && current == receipt.id()
                    ));
                    assert!(matches!(
                        mailbox.try_reserve(),
                        Err(Error::Queue(Vacancy::Full))
                    ));
                    assert_eq!(previous.get(), 0);
                    drops.set(drops.get() + 1);
                }))
            };
            let (cause, _) = mailbox
                .try_settle(&receipt, rejected)
                .err()
                .expect("settled owner rejects another outcome");
            assert!(matches!(
                cause,
                Error::Slot(Conflict::Settled { key, current })
                    if key == receipt.id() && current == receipt.id()
            ));
            assert_eq!(drops.get(), 1);
            assert_eq!(previous.get(), 0);
            assert!(matches!(lease.cancel(), Cancellation::Settled(_)));
            assert_eq!(previous.get(), 1);
            assert!(mailbox.try_reserve().is_ok());
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        scoped,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn reclaims_forgotten() {
    let mut reports = Vec::new();

    Driver::new(Runtime::throughput().config()).with_scope(
        |scope| {
            let pending = Rc::new(Cell::new(0));
            let context = Rc::new(Cell::new(0));
            let outcome = Rc::new(Cell::new(0));
            {
                let mailbox = (Domain::<tcp::Tcp, Dropped<'_>, Dropped<'_>>::try_new(
                    Count::new(2),
                    &mut scope.context().storage,
                ))
                .expect("mailbox source");
                let lease = (mailbox.try_reserve())
                    .expect("forgotten pending attempt")
                    .start(pending.counted(), Loopback(0).target());
                mem::forget(lease);
                let lease = (mailbox.try_reserve())
                    .expect("forgotten settled attempt")
                    .start(context.counted(), Loopback(0).target());
                assert!(
                    mailbox
                        .try_settle(&lease.receipt(), outcome.counted())
                        .is_ok()
                );
                mem::forget(lease);
                assert_eq!(pending.get(), 0);
                assert_eq!(context.get(), 1);
                assert_eq!(outcome.get(), 0);
            }
            assert_eq!(pending.get(), 1);
            assert_eq!(context.get(), 1);
            assert_eq!(outcome.get(), 1);
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        scoped,
    );
    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

#[test]
fn retains_rejected_outcome() {
    let mut reports = Vec::new();
    let plan = Mailbox::<Box<u64>>::new(Count::new(1));
    let config = (Needs::from_tree(&plan).and_then(|needs| needs.resolve::<Throughput>()))
        .expect("mailbox requirements");
    use dope_core::driver::life::creation::Attempt;
    let mut create = Attempt::new();
    let executor = {
        let result = Executor::new(&mut create, config);
        result
            .as_ref()
            .map_err(|error| error.kind())
            .expect("native creation");
        result.ok().expect("native creation")
    };
    executor.with_plan(plan).run(|executor| {
        prepared(executor.try_create(
            |scope| {
                let mailbox = scope.storage();
                let vacant = (mailbox.try_reserve()).expect("initial attempt");
                let lease = vacant.start(7, Loopback(0).target());
                let key = lease.id();
                let done = lease.receipt();
                assert!(matches!(lease.cancel(), Cancellation::Pending));
                let vacant = (mailbox.try_reserve()).expect("replacement attempt");
                let lease = vacant.start(9, Loopback(0).target());
                let current = lease.id();
                assert!((key.index()) == (current.index()));
                assert!(key != current);

                let value = Box::new(41);
                let address = ptr::from_ref(value.as_ref());
                let (rejected, cost) = Tracker::usage(|| mailbox.try_settle(&done, value));
                assert_eq!(cost, (0, 0));
                assert!(
                    rejected.is_err(),
                    "the retired generation must reject its outcome"
                );
                let (cause, value) = rejected
                    .err()
                    .expect("retired generation must reject outcome");
                assert!((cause) == (Error::Slot(Conflict::Pending { key, current })));
                assert_eq!(ptr::from_ref(value.as_ref()), address);
                assert!(matches!(
                    mailbox.try_settle(&lease.receipt(), value),
                    Ok((9, None))
                ));
                let (cancel, cost) = Tracker::usage(|| lease.cancel());
                assert_eq!(cost, (0, 0));
                let value = match cancel {
                    Cancellation::Settled(value) => Some(value),
                    _ => None,
                }
                .expect("cancellation must return the owned outcome");
                assert_eq!(ptr::from_ref(value.as_ref()), address);
                assert_eq!(*value, 41);
            },
            recovery::Policy::new(|event| reports.push(event), recover),
        ));
    });

    assert!(
        reports.is_empty(),
        "unexpected reclaimed operations: {reports:?}"
    );
}

fn prepared<O>(result: Result<(), domain::Error<'_, domain::Rejected<creation::Error, O>, ()>>) {
    result
        .as_ref()
        .map_err(|error| error.cause().map_storage(|rejected| &rejected.cause))
        .expect("native storage");
}

fn scoped(result: Result<(), scope::Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(scope::Error::cause)
        .expect("driver lifecycle");
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
