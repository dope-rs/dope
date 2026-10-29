use std::{marker, mem, ops, pin};

use dope_core::{
    driver::{retained, schedule, settings},
    io,
};
use o3::base::cell::region;

use super::forward;
use crate::{executor, topology::lending};

#[pin_project::pin_project]
#[repr(transparent)]
pub struct Owner<S, T> {
    #[pin]
    stored: S,
    app: marker::PhantomData<T>,
}

impl<S, T> Owner<S, T> {
    pub(super) const fn new(stored: S) -> Self {
        Self {
            stored,
            app: marker::PhantomData,
        }
    }
}

// SAFETY: the pinned stored projection remains unchanged for the entire lifecycle.
unsafe impl<S: lending::raw::Project<App = T>, T> lending::raw::Project for Owner<S, T> {
    type App = T;

    fn shared(self: pin::Pin<&Self>) -> pin::Pin<&Self::App> {
        self.project_ref().stored.shared()
    }

    fn exclusive(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::App> {
        self.project().stored.exclusive()
    }
}

impl<'d, S, T> executor::raw::Layout<'d> for Owner<S, T>
where
    T: executor::raw::Layout<'d>,
{
    type ParkPlan = <T as executor::raw::Layout<'d>>::ParkPlan;
}

// SAFETY: lifecycle traversal retains one exact pinned application. Its storage
// is released once after final traversal, before its enclosing owner is finished.
unsafe impl<'d, S, T> executor::raw::App<'d> for Owner<S, T>
where
    S: lending::raw::Project<App = T>,
    T: executor::raw::App<'d>,
    Self: executor::raw::Finishes<'d, Self>,
{
    const ROUTES: settings::Routes = T::ROUTES;
    const WAKE_ROUTES: settings::Routes = T::WAKE_ROUTES;
    const COORDS: usize = <T as executor::raw::App<'d>>::COORDS;

    fn select_park() -> Option<Self::ParkPlan> {
        <T as executor::raw::App<'d>>::select_park()
    }

    fn prepare_park(
        app: pin::Pin<&Self>,
        plan: &mut Self::ParkPlan,
    ) -> Option<executor::raw::Count> {
        <T as executor::raw::App<'d>>::prepare_park(lending::raw::Project::shared(app), plan)
    }

    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: this owner preserves the same prepared plan and pinned projection.
        unsafe { forward::park(app, plan, park) }
    }

    unsafe fn activate<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut Self>,
        activate: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: this owner retains the installed application and its wake routes.
        unsafe { forward::activate(app, activate) }
    }

    fn admit(
        app: pin::Pin<&Self>,
        needs: &mut settings::claim::Needs,
    ) -> Result<(), settings::errors::Claim> {
        T::admit(lending::raw::Project::shared(app), needs)
    }

    fn released(released: executor::raw::Released<'_, Self>) -> executor::raw::Released<'_, Self> {
        // SAFETY: this wrapper retains the exact original pinned application.
        unsafe { released.visit(lending::raw::Project::exclusive) }
    }

    fn install<'app, 'owner>(
        call: executor::raw::Install<'app, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Self> {
        forward::install(call)
    }

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Dispatch<'app, 'driver, 'turn, 'root, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root,
    {
        forward::dispatch(call)
    }

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        call: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        forward::shutdown(call)
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        <Self as executor::raw::Finishes<'d>>::call(call)
    }

    fn progress(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        T::progress(lending::raw::Project::shared(app), view)
    }

    fn stopping(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        T::stopping(lending::raw::Project::shared(app), view)
    }
}

impl<'d, T> executor::raw::Finishes<'d, Self> for lending::raw::Borrowed<'_, T>
where
    T: executor::raw::App<'d>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        forward::finish(call)
    }
}

impl<'d, T> executor::raw::Finishes<'d, Self> for Owner<mem::ManuallyDrop<T>, T>
where
    T: executor::raw::App<'d>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut app, install, finish) = forward::finish(call).into_parts();
        // SAFETY: final traversal completed and this private lifecycle wrapper
        // is never projected again before its enclosing application is destroyed.
        unsafe { mem::ManuallyDrop::drop(app.as_mut().project().stored.get_unchecked_mut()) };
        executor::raw::Finish::new(app, install, finish)
    }
}

// SAFETY: every drain call projects the same pinned application; its Drain
// contract preserves the admitted owners and route claims.
unsafe impl<'d, S, T> executor::raw::Drain<'d> for Owner<S, T>
where
    S: lending::raw::Project<App = T>,
    T: executor::raw::Drain<'d>,
    Self: executor::raw::Finishes<'d, Self>,
{
    fn begin(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        executor::raw::Drain::begin(lending::raw::Project::exclusive(self), turn, driver);
    }

    fn progress(
        self: pin::Pin<&Self>,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        executor::raw::Drain::progress(lending::raw::Project::shared(self), region)
    }

    fn force(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        executor::raw::Drain::force(lending::raw::Project::exclusive(self), turn, driver);
    }
}
