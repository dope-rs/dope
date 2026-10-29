use std::{
    convert::Infallible, env::consts::OS, mem, net::TcpStream, ops::ControlFlow, time::Duration,
};

use dope_core::{
    driver::{
        life::recovery,
        result::poll,
        retained::{self, accept, arm, flight},
        route::{Epoch, KeyTag, indices::SlotIndex, kind},
        settings,
    },
    io::{
        event::{
            Kind,
            batch::Flow,
            net::accept::{Failure, Outcome},
        },
        socket::{Listener, install::Install},
    },
};
use dope_sys::{io::socket::accept as native, table::registry};
use dope_test::scope::Error;

use crate::common::rt::Runtime;

pub mod common;

const _: () = assert!(!mem::needs_drop::<
    arm::Arming<'static, 'static, KeyTag<0, kind::Accept>>,
>());

#[test]
fn failure_provenance() {
    for cause in [
        registry::Failure::Slot,
        registry::Failure::TargetOccupied,
        registry::Failure::TargetFull,
        registry::Failure::FileOccupied,
    ] {
        let failure = Failure::from(native::Error::Registration(cause));
        assert_eq!(failure, Failure::Registration(cause));
        assert!(!failure.is_resource());
        assert!(!failure.is_retryable());
    }
    for (failure, resource) in [
        (
            Failure::Syscall(dope_core::Errno::try_from(libc::ENFILE).expect("errno")),
            true,
        ),
        (
            Failure::Syscall(dope_core::Errno::try_from(libc::EMFILE).expect("errno")),
            OS == "macos",
        ),
        (Failure::Capacity { close: Ok(()) }, true),
        (
            Failure::Capacity {
                close: Err(dope_core::Errno::BAD_DESCRIPTOR),
            },
            false,
        ),
    ] {
        assert_eq!(failure.is_resource(), resource, "{failure:?}");
    }
}

#[test]
fn pending_accept_identity() {
    Runtime::quic::<2, 8>()
        .file_slots(settings::FileSlots::fixed::<1, 16>())
        .with_scope(
            |scope| {
                let mut driver = scope.context();
                let target = driver
                    .driver_ref()
                    .targets::<KeyTag<0, kind::Accept>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL);
                let token = target.dispatch().erase_route();
                {
                    let mut state = arm::State::new(target);
                    assert!(state.begin().is_some());
                    assert!(state.begin().is_some());
                    assert!(state.begin_retry().is_some());
                    assert!(state.begin().is_none());
                    assert!(state.begin_retry().is_some());
                }
                let listener = Listener::bind(common::LOCALHOST).expect("listener");
                let (listener, address) =
                    Install::adopt_bound(driver.reborrow(), listener).expect("install listener");
                let flights = driver
                    .storage
                    .reborrow()
                    .try_flights(flight::Need::<KeyTag<0, kind::Accept>>::new(
                        flight::Capacity::fixed::<1>(),
                    ))
                    .expect("accept flight")
                    .slots();
                let mut lanes =
                    accept::Slots::new(vec![accept::Lane::new(target)].into_boxed_slice());

                if OS == "linux" {
                    use dope_core::driver::schedule::Progress;

                    let failure =
                        Failure::Syscall(dope_core::Errno::try_from(libc::ENFILE).expect("errno"));
                    assert!(!failure.can_defer(&mut driver));
                    let retiring = Listener::bind(common::LOCALHOST).expect("retiring listener");
                    let (retiring, _) = Install::adopt_bound(driver.reborrow(), retiring)
                        .expect("install retiring listener");
                    drop(retiring);
                    assert!(failure.can_defer(&mut driver));
                    assert!(matches!(
                        accept::Readiness::from(driver.driver_ref()).progress(&driver.regions().0),
                        Progress::Runnable
                    ));
                    scope.with_driver(|mut driver, mut schedule| {
                        let mut turn = schedule.begin();
                        assert!(matches!(
                            driver.commit(turn.reactor()),
                            Ok(dope_core::driver::result::poll::Commit::Drained)
                        ));
                        assert!(matches!(
                            accept::Readiness::from(driver.driver_ref())
                                .progress(&driver.regions().0),
                            Progress::Waiting(_)
                        ));
                        assert!(failure.can_defer(&mut driver));
                        let mut state = arm::State::new(target);
                        assert!(matches!(
                            state
                                .begin()
                                .expect("state")
                                .accept(&mut driver, &flights, &listener),
                            arm::Submission::Deferred
                        ));
                        assert!(!state.is_live());
                        let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                        let mut driver = retained::Context::new(driver, owner);
                        let arming = lanes
                            .get_mut(0)
                            .expect("accept lane")
                            .begin()
                            .expect("idle lane");
                        // SAFETY: the pinned lane and listener remain live until quiescence.
                        assert!(matches!(
                            unsafe { arming.submit(&mut driver, &flights, &listener) },
                            arm::Submission::Deferred
                        ));
                        assert_eq!(lanes.active(), 0);
                    });
                    scope.quiesce();
                    assert!(!failure.can_defer(&mut scope.context()));
                }

                for _ in 0..2 {
                    let driver = scope.context();
                    let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                    let mut driver = retained::Context::new(driver, owner);
                    let arming = lanes
                        .get_mut(0)
                        .expect("accept lane")
                        .begin()
                        .expect("idle lane");
                    // SAFETY: lanes and listener remain live at their submitted
                    // addresses until this iteration's native quiescence completes.
                    assert!(matches!(
                        unsafe { arming.submit(&mut driver, &flights, &listener) },
                        arm::Submission::Armed
                    ));
                    assert!(matches!(
                        lanes.get_mut(0).expect("accept lane").retire(token),
                        accept::Retire::Stale
                    ));
                    assert_eq!(lanes.active(), 1);

                    scope.quiesce();
                    let terminal = match lanes.get_mut(0).expect("accept lane").retire(token) {
                        accept::Retire::Done(accept::Retired::Terminal(terminal)) => Some(terminal),
                        accept::Retire::Stale
                        | accept::Retire::Done(accept::Retired::Cancelled) => None,
                    }
                    .expect("native terminal must release the lane");
                    terminal.discard();
                    {
                        let pending = flights
                            .accept(
                                scope.context(),
                                &listener,
                                target.operation::<kind::Accept>(),
                            )
                            .expect("detached accept");
                        assert!(pending.matches(token));
                    }
                    assert!(scope.try_boot().is_err());
                    scope.quiesce();
                    scope.quiesce();
                    assert!(scope.try_boot().is_ok());
                }

                let failure =
                    Failure::Syscall(dope_core::Errno::try_from(libc::ENFILE).expect("errno"));
                let mut fd = None;
                {
                    let driver = scope.context();
                    let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                    let mut driver = retained::Context::new(driver, owner);
                    let arming = lanes.get_mut(0).expect("lane").begin().expect("idle");
                    // SAFETY: the lane and listener remain pinned until the terminal below.
                    assert!(matches!(
                        unsafe { arming.submit(&mut driver, &flights, &listener) },
                        arm::Submission::Armed
                    ));
                }
                let peer = TcpStream::connect(address).expect("peer");
                scope.with_driver(|mut driver, mut schedule| {
                    let mut turn = schedule.begin();
                    driver
                        .wait(turn.reactor(), Some(Duration::from_secs(1)))
                        .expect("accept");
                    let dispatch = driver.dispatch(turn.reactor(), |event, _| {
                        let done = match event.into_kind() {
                            Kind::Accept(done) => Some(done),
                            _ => None,
                        }
                        .expect("accept event");
                        let (_, flow, outcome) = done.into_parts();
                        assert_eq!(flow, Flow::Last);
                        let slot = match outcome {
                            Outcome::Accepted(slot) => Ok(slot),
                            Outcome::Failed(error) => Err(error),
                        }
                        .expect("accepted slot");
                        fd = Some(slot.bind());
                        ControlFlow::Continue(())
                    });
                    assert!(matches!(
                        dispatch,
                        poll::Dispatch::Ready { retained: None, .. }
                    ));
                });
                assert!(fd.is_some());
                let capacity = Failure::Capacity { close: Ok(()) };
                assert!(!capacity.can_defer(&mut scope.context()));
                let spare = Listener::bind(common::LOCALHOST).expect("outbound listener");
                let (spare, _) =
                    Install::adopt_bound(scope.context(), spare).expect("outbound install");
                drop(spare);
                assert!(!capacity.can_defer(&mut scope.context()));
                scope.quiesce();
                let done = match lanes.get_mut(0).expect("lane").retire(token) {
                    accept::Retire::Done(accept::Retired::Terminal(done)) => Some(done),
                    _ => None,
                }
                .expect("terminal accept");
                done.discard();
                for _ in 0..2 {
                    {
                        let driver = scope.context();
                        let owner = retained::raw::Owner::from_driver(driver.driver_ref());
                        let mut driver = retained::Context::new(driver, owner);
                        let arming = lanes.get_mut(0).expect("lane").begin().expect("idle");
                        // SAFETY: the lane and listener remain pinned until quiescence below.
                        assert!(matches!(
                            unsafe { arming.submit(&mut driver, &flights, &listener) },
                            arm::Submission::Armed
                        ));
                    }
                    let changed = match fd.take() {
                        Some(fd) => {
                            assert_eq!(fd.index(), SlotIndex::ZERO);
                            // Queue the return; the capacity check must settle it before querying.
                            drop(fd);
                            assert_eq!(capacity.can_defer(&mut scope.context()), OS == "macos");
                            assert!(
                                !Failure::Capacity {
                                    close: Err(dope_core::Errno::BAD_DESCRIPTOR),
                                }
                                .can_defer(&mut scope.context())
                            );
                            true
                        }
                        None => {
                            let spare = Listener::bind(common::LOCALHOST).expect("spare");
                            let (spare, _) = Install::adopt_bound(scope.context(), spare)
                                .expect("spare install");
                            spare.close(&mut scope.context());
                            false
                        }
                    };
                    scope.quiesce();
                    assert!(!failure.can_defer(&mut scope.context()));
                    let done = match lanes.get_mut(0).expect("lane").retire(token) {
                        accept::Retire::Done(accept::Retired::Terminal(done)) => Some(done),
                        _ => None,
                    }
                    .expect("terminal lane");
                    assert_eq!(
                        done.can_defer(&failure, &mut scope.context()),
                        changed && OS == "linux"
                    );
                    // A completed close does not replace the backend's local-slot proof.
                    assert_eq!(
                        done.can_defer(&capacity, &mut scope.context()),
                        OS == "macos"
                    );
                    done.discard();
                }
                drop(peer);

                let mut state = arm::State::new(target);
                assert!(matches!(
                    state
                        .begin()
                        .expect("state")
                        .accept(&mut scope.context(), &flights, &listener),
                    arm::Submission::Armed
                ));
                state.stop(&mut scope.context());
                assert!(matches!(
                    state.complete(token, Flow::Last),
                    arm::Disposition::Discard
                ));
                state.restart();
                assert!(state.begin().is_none());
                scope.quiesce();
                assert!(matches!(
                    state.complete(token, Flow::Last),
                    arm::Disposition::Discard
                ));
                state.restart();
                assert!(state.begin().is_some());
                listener.close(&mut scope.context());
            },
            recovery::Policy::new(|_| {}, recover),
            finish,
        );
}

fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native retirement: {:?}", failure.cause())
}

fn finish(result: Result<(), Error<'_, ()>>) {
    result
        .as_ref()
        .map_err(Error::cause)
        .expect("driver lifecycle");
}
