use std::{ops, pin};

use dope_core::io;

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

pub(super) fn dispatch<'app, 'driver, 'turn, 'root, 'd: 'root, P>(
    call: executor::raw::Dispatch<'app, 'driver, 'turn, 'root, 'd, P>,
) -> ops::ControlFlow<io::Event<'d>>
where
    P: lending::raw::Project + 'root,
    P::App: executor::raw::App<'d> + 'root,
{
    call.map(|app, event, turn, driver| {
        <P::App as executor::raw::App<'d>>::dispatch(executor::raw::Dispatch::new(
            app.exclusive(),
            event,
            turn,
            driver,
        ))
    })
    .map_break(executor::raw::Dispatch::into_event)
}

/// # Safety
/// The prepared plan and park authority belong to this exact pinned projection.
pub(super) unsafe fn park<'app, 'driver, 'turn, 'root, 'd, P: lending::raw::Project>(
    app: pin::Pin<&'app mut P>,
    plan: &mut <P::App as executor::raw::Layout<'d>>::ParkPlan,
    park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
) where
    'd: 'driver + 'turn + 'root,
    P::App: executor::raw::App<'d>,
{
    // SAFETY: preparation and invocation use the same application projection.
    unsafe { <P::App as executor::raw::App<'d>>::park(app.exclusive(), plan, park) }
}

/// # Safety
/// The activation retains the installed root and route of this exact projection.
pub(super) unsafe fn activate<'app, 'driver, 'turn, 'root, 'd, P: lending::raw::Project>(
    app: pin::Pin<&'app mut P>,
    activate: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
) -> bool
where
    'd: 'driver + 'turn + 'root,
    P::App: executor::raw::App<'d>,
{
    // SAFETY: the caller retains the installed root and route through this projection.
    unsafe { <P::App as executor::raw::App<'d>>::activate(app.exclusive(), activate) }
}

pub(super) fn shutdown<'app, 'turn, 'driver, 'owner, 'd: 'owner, P: lending::raw::Project>(
    call: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, P>,
) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, P>
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
