use std::{ops, pin};

use dope_core::driver::{self, schedule};

use crate::{executor, topology::lending};

pub(super) fn install<'app, 'owner, 'd, P: lending::raw::Project>(
    call: executor::raw::Install<'app, 'owner, 'd, P>,
) -> executor::raw::Install<'app, 'owner, 'd, P>
where
    P::App: executor::raw::App<'d>,
{
    let (mut app, install) = call.into_parts();
    let call = executor::raw::Install::new(app.as_mut().exclusive(), install);
    let install = <P::App as executor::raw::App<'d>>::install(call).into_install();
    executor::raw::Install::new(app, install)
}

pub(super) fn dispatch<'event, 'd, P>(
    app: pin::Pin<&mut P>,
    event: executor::raw::Dispatch<'event, 'd>,
    turn: schedule::Turn<'_, 'd>,
    driver: driver::Context<'_, 'd>,
) -> ops::ControlFlow<executor::raw::Dispatch<'event, 'd>>
where
    P: lending::raw::Project,
    P::App: executor::raw::App<'d>,
{
    <P::App as executor::raw::App<'d>>::dispatch(app.exclusive(), event, turn, driver)
}

/// # Safety
/// The prepared plan and park authority belong to this exact pinned projection.
pub(super) unsafe fn park<'app, 'driver, 'turn, 'd, P: lending::raw::Project>(
    app: pin::Pin<&'app mut P>,
    plan: &mut <P::App as executor::raw::Layout<'d>>::ParkPlan,
    park: executor::raw::Park<'driver, 'turn, 'd>,
) where
    'd: 'driver + 'turn,
    P::App: executor::raw::App<'d>,
{
    // SAFETY: preparation and invocation use the same application projection.
    unsafe { <P::App as executor::raw::App<'d>>::park(app.exclusive(), plan, park) }
}

/// # Safety
/// The activation retains the installed root and route of this exact projection.
pub(super) unsafe fn activate<'app, 'driver, 'turn, 'd, P: lending::raw::Project>(
    app: pin::Pin<&'app mut P>,
    activate: executor::raw::Activate<'driver, 'turn, 'd>,
) -> bool
where
    'd: 'driver + 'turn,
    P::App: executor::raw::App<'d>,
{
    // SAFETY: the caller retains the installed root and route through this projection.
    unsafe { <P::App as executor::raw::App<'d>>::activate(app.exclusive(), activate) }
}

pub(super) fn shutdown<'app, 'turn, 'driver, 'd, P: lending::raw::Project>(
    call: executor::raw::Shutdown<'app, 'turn, 'driver, 'd, P>,
) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'd, P>
where
    P::App: executor::raw::App<'d>,
{
    let (mut app, turn, driver) = call.into_parts();
    let call = executor::raw::Shutdown::new(app.as_mut().exclusive(), turn, driver);
    let (turn, driver) = <P::App as executor::raw::App<'d>>::shutdown(call).into_drive();
    executor::raw::Shutdown::new(app, turn, driver)
}

pub(super) fn finish<'app, 'finish, 'owner, 'epoch, 'd, P: lending::raw::Project>(
    call: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, P>,
) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, P>
where
    P::App: executor::raw::App<'d>,
{
    let (mut app, install, finish) = call.into_parts();
    let call = executor::raw::Finish::new(app.as_mut().exclusive(), install, finish);
    let (install, finish) = <P::App as executor::raw::App<'d>>::finish(call).into_finish();
    executor::raw::Finish::new(app, install, finish)
}
