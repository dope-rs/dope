mod tests;

use std::{ops, pin};

use dope_core::driver;

pub(super) fn activate<'d, const ID: u8>(
    creating: dope_core::io::fd::handles::CreatingSocket<'d>,
    owner: dope_core::driver::route::Target<'d, dope_core::driver::route::KeyTag<ID>>,
    completion: dope_core::io::event::creation::Completion<'d>,
) -> dope_core::io::fd::handles::Descriptor<'d> {
    let (target, event) = completion.into_parts();
    assert!(
        owner
            .operation(dope_core::driver::route::kind::SOCKET)
            .matches(target),
        "socket completion must retain its exact routed owner",
    );
    let dope_core::io::SocketEvent::Created(created) = event else {
        panic!("socket creation failed")
    };
    match created.activate(creating) {
        Ok(fd) => fd,
        Err(_) => panic!("socket completion did not match its creating authority"),
    }
}

pub(super) fn scope<R>(
    driver: pin::Pin<&mut driver::Driver>,
    f: impl for<'d> FnOnce(driver::lifecycle::Scope<'d>) -> R,
) -> R {
    // SAFETY: the generative scope consumes every safe domain borrow before return.
    let owner = unsafe { driver::lifecycle::quiesce::raw::Owner::new() };
    driver.scope(driver::lifecycle::quiesce::Lease::new(owner), f)
}

pub(super) fn owner() -> driver::lifecycle::quiesce::Lease {
    // SAFETY: the caller immediately transfers this proof into a generative
    // Domain entry which owns the driver until all domain borrows are gone.
    driver::lifecycle::quiesce::Lease::new(unsafe { driver::lifecycle::quiesce::raw::Owner::new() })
}

pub(super) fn with_turn<'d, R>(
    scope: &mut driver::lifecycle::Scope<'d>,
    f: impl for<'context, 'turn> FnOnce(
        driver::Context<'context, 'd>,
        &mut driver::schedule::ActiveTurn<'turn, 'd>,
    ) -> R,
) -> R {
    scope.with_turn(|_, context, mut controller| {
        let mut turn = controller.begin(driver::schedule::MAX_TURN_WORK_BUDGET);
        f(context, &mut turn)
    })
}

pub(super) fn with_controller<'d, R>(
    scope: &mut driver::lifecycle::Scope<'d>,
    f: impl for<'a> FnOnce(driver::Context<'a, 'd>, driver::schedule::Controller<'a, 'd>) -> R,
) -> R {
    scope.with_turn(|_, context, turn| f(context, turn))
}

pub(super) fn dispatch_all<'d, S>(
    driver: &mut S,
    work: driver::schedule::Reactor<'_, 'd>,
    mut dispatch: impl FnMut(dope_core::io::Event<'d>),
) -> driver::ops::poll::Drain
where
    S: driver::ops::poll::Source<'d>,
{
    let dispatched = driver::ops::poll::Source::dispatch(driver, work, |event, _driver| {
        dispatch(event);
        ops::ControlFlow::Continue(())
    });
    let (drain, retained) = dispatched.into_parts();
    assert!(retained.is_none());
    drain
}

pub(super) fn submit_recv<'d, Tag: driver::route::Tag>(
    driver: &mut driver::Context<'_, 'd>,
    fd: &dope_core::io::fd::handles::Descriptor<'d>,
    target: driver::route::Target<'d, Tag>,
) -> Result<(), driver::SubmitError> {
    let slots = driver
        .flight_slots::<Tag>(1)
        .map_err(|_| driver::SubmitError)?;
    driver::ops::Submit::submit_recv(driver, &slots, fd, target).map(drop)
}

pub(super) fn open_fds() -> Vec<libc::c_int> {
    (0..4096).filter(|fd| is_open(*fd)).collect()
}

pub(super) fn is_open(fd: libc::c_int) -> bool {
    // SAFETY: F_GETFD only queries the integer descriptor and writes no memory.
    unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
}
