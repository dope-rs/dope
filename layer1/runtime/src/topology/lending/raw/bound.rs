use std::{mem, ops, pin, ptr};

use dope_core::{
    driver::{retained, schedule, settings},
    io,
};
use o3::base::cell::region;

use super::forward;
use crate::{executor, topology::lending};

/// A dependent application retained by its pinned provider owner.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct Bound<'app, 'd, H, T> {
    #[pin]
    pub(super) inner: lending::raw::Pair<'app, 'd, H, T>,
}

impl<'app, 'd, H, T> Bound<'app, 'd, H, T> {
    pub(crate) const fn new(head: pin::Pin<&'app mut H>, tail: T) -> Self {
        const {
            assert!(mem::size_of::<Self>() == mem::size_of::<(pin::Pin<&'app mut H>, T)>());
            assert!(mem::align_of::<Self>() == mem::align_of::<(pin::Pin<&'app mut H>, T)>());
            assert!(!mem::needs_drop::<Self>());
        }
        Self {
            inner: lending::raw::Pair::new(head, tail),
        }
    }

    pub fn tail(self: pin::Pin<&Self>) -> pin::Pin<&T> {
        self.project_ref().inner.tail()
    }
}

// SAFETY: Bound retains this exact pinned Pair for its complete lifecycle.
unsafe impl<'app, 'd, H, T> lending::raw::Project for Bound<'app, 'd, H, T> {
    type App = lending::raw::Pair<'app, 'd, H, T>;

    fn shared(self: pin::Pin<&Self>) -> pin::Pin<&Self::App> {
        self.project_ref().inner
    }

    fn exclusive(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::App> {
        self.project().inner
    }
}

impl<'app, 'd, H, T> executor::raw::Layout<'d> for Bound<'app, 'd, H, T>
where
    H: executor::raw::Layout<'d>,
    T: executor::raw::Layout<'d>,
{
    type ParkPlan = <lending::raw::Pair<'app, 'd, H, T> as executor::raw::Layout<'d>>::ParkPlan;
}

// SAFETY: one exclusive execution owns both projected applications through
// finish. Stop preserves borrowed storage. Finish destroys the dependent tail
// before the provider's destructive traversal, with no subsequent tail access.
unsafe impl<'app, 'd, H, T> executor::raw::App<'d> for Bound<'app, 'd, H, T>
where
    H: executor::raw::App<'d>,
    T: executor::raw::App<'d>,
{
    const ROUTES: settings::Routes =
        <lending::raw::Pair<'app, 'd, H, T> as executor::raw::App<'d>>::ROUTES;
    const WAKE_ROUTES: settings::Routes =
        <lending::raw::Pair<'app, 'd, H, T> as executor::raw::App<'d>>::WAKE_ROUTES;
    const COORDS: usize = <lending::raw::Pair<'app, 'd, H, T> as executor::raw::App<'d>>::COORDS;

    fn select_park() -> Option<Self::ParkPlan> {
        <lending::raw::Pair<'app, 'd, H, T> as executor::raw::App<'d>>::select_park()
    }

    fn prepare_park(
        app: pin::Pin<&Self>,
        plan: &mut Self::ParkPlan,
    ) -> Option<executor::raw::Count> {
        <lending::raw::Pair<'app, 'd, H, T> as executor::raw::App<'d>>::prepare_park(
            lending::raw::Project::shared(app),
            plan,
        )
    }

    unsafe fn park<'call, 'driver, 'turn, 'root>(
        app: pin::Pin<&'call mut Self>,
        plan: &mut Self::ParkPlan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: this bound retains the same prepared plan and pinned pair.
        unsafe { forward::park(app, plan, park) }
    }

    unsafe fn activate<'call, 'driver, 'turn, 'root>(
        app: pin::Pin<&'call mut Self>,
        activate: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: this bound retains the installed pair and its wake routes.
        unsafe { forward::activate(app, activate) }
    }

    fn admit(
        app: pin::Pin<&Self>,
        needs: &mut settings::claim::Needs,
    ) -> Result<(), settings::errors::Claim> {
        <lending::raw::Pair<'_, 'd, H, T> as executor::raw::App<'d>>::admit(
            lending::raw::Project::shared(app),
            needs,
        )
    }

    fn released(released: executor::raw::Released<'_, Self>) -> executor::raw::Released<'_, Self> {
        // SAFETY: only the owned tail has ended external client access. Its
        // provider remains borrowed until the tail and all construction state drop.
        unsafe { released.visit(|app| app.project().inner.project().tail) }
    }

    fn install<'call, 'owner>(
        call: executor::raw::Install<'call, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'call, 'owner, 'd, Self> {
        forward::install(call)
    }

    fn dispatch<'call, 'driver, 'turn, 'root>(
        call: executor::raw::Dispatch<'call, 'driver, 'turn, 'root, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root,
    {
        forward::dispatch(call)
    }

    fn progress(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        <lending::raw::Pair<'_, 'd, H, T> as executor::raw::App<'d>>::progress(
            lending::raw::Project::shared(app),
            view,
        )
    }

    fn stopping(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        <lending::raw::Pair<'_, 'd, H, T> as executor::raw::App<'d>>::stopping(
            lending::raw::Project::shared(app),
            view,
        )
    }

    fn shutdown<'call, 'turn, 'driver, 'owner>(
        call: executor::raw::Shutdown<'call, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'call, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        let (mut app, turn, driver) = call.into_parts();
        let tail = app.as_mut().project().inner.project().tail;
        let call = executor::raw::Shutdown::new(tail, turn, driver);
        let (turn, driver) = forward::shutdown(call).into_drive();
        let head = app.as_mut().project().inner.project().head;
        let call = executor::raw::Shutdown::new(head, turn, driver);
        let (turn, driver) = forward::shutdown(call).into_drive();
        executor::raw::Shutdown::new(app, turn, driver)
    }

    fn finish<'call, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'call, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'call, 'finish, 'owner, 'epoch, 'd, Self> {
        let (mut app, install, finish) = call.into_parts();
        let tail = app.as_mut().project().inner.project().tail;
        let call = executor::raw::Finish::new(tail, install, finish);
        let (install, finish) =
            <lending::raw::Owner<mem::ManuallyDrop<T>, T> as executor::raw::Finishes<'d>>::call(
                call,
            )
            .into_finish();
        let mut head = app.as_mut().project().inner.project().head;
        let _ = <lending::raw::Borrowed<'_, H> as executor::raw::App<'d>>::released(
            executor::raw::Released::new(head.as_mut()),
        );
        let call = executor::raw::Finish::new(head, install, finish);
        let (install, finish) =
            <lending::raw::Borrowed<'_, H> as executor::raw::Finishes<'d>>::call(call)
                .into_finish();
        executor::raw::Finish::new(app, install, finish)
    }
}

impl<'parent, 'd, H, T> Bound<'parent, 'd, H, T>
where
    H: executor::raw::App<'d>,
    T: executor::raw::App<'d>,
{
    /// # Safety
    /// Consume this exact native-quiesced owner; never project or finish its backing again.
    pub(crate) unsafe fn finish_tail<'call, 'finish, 'owner, 'epoch>(
        call: executor::raw::Finish<'call, 'finish, 'owner, 'epoch, 'd, Self>,
        plan: Option<<Self as executor::raw::Layout<'d>>::ParkPlan>,
    ) -> (
        lending::raw::Borrowed<'parent, H>,
        Option<<H as executor::raw::Layout<'d>>::ParkPlan>,
    ) {
        let (mut app, install, finish) = call.into_parts();
        let tail = app.as_mut().project().inner.project().tail;
        let call = executor::raw::Finish::new(tail, install, finish);
        let _ = <lending::raw::Owner<mem::ManuallyDrop<T>, T> as executor::raw::Finishes<'d>>::call(
            call,
        );
        let head = app.as_mut().project().inner.project().head;
        // SAFETY: the consumed owner makes its old backing inaccessible. Moving
        // this Pin handle preserves the actual provider's address and full loan.
        let head = unsafe { ptr::read(head.get_unchecked_mut()) };
        (head, plan.and_then(|plan| plan.3))
    }
}

// SAFETY: drain forwards through the same pinned Pair; its designated tail
// preserves every admitted owner while the borrowed provider stays installed.
unsafe impl<'d, H, T> executor::raw::Drain<'d> for Bound<'_, 'd, H, T>
where
    H: executor::raw::App<'d>,
    T: executor::raw::Drain<'d>,
{
    fn begin(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        executor::raw::Drain::begin(self.project().inner, turn, driver);
    }

    fn progress(
        self: pin::Pin<&Self>,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        executor::raw::Drain::progress(self.project_ref().inner, region)
    }

    fn force(
        self: pin::Pin<&mut Self>,
        turn: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        executor::raw::Drain::force(self.project().inner, turn, driver);
    }
}

const _: () = {
    assert!(mem::size_of::<Bound<'_, '_, (), ()>>() == mem::size_of::<&mut ()>());
    assert!(mem::align_of::<Bound<'_, '_, (), ()>>() == mem::align_of::<&mut ()>());
    assert!(!mem::needs_drop::<Bound<'_, '_, (), ()>>());
};
