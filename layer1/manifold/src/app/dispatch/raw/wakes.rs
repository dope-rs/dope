use dope_core::driver::{route, settings};
use dope_runtime::executor;

use crate::app::dispatch;

/// Activates the exact installed owner selected for this target's route.
///
/// # Safety
/// The call must retain the installed owner selected for `R`.
unsafe fn activate<'a, 'driver, 'turn, 'root, 'd, M, R>(
    call: executor::raw::Call<'a, M, (), executor::raw::Activate<'driver, 'turn, 'root, 'd>>,
) where
    'd: 'driver + 'turn + 'root,
    M: dispatch::raw::Activate<'d, R>,
    R: route::App,
{
    let (owner, (), wake) = call.into_parts();
    wake.map(|target, turn, driver| {
        let target = driver
            .driver_ref()
            .targets::<R>()
            .bind(target.slot(), target.epoch());
        let mut root = dispatch::raw::Context::<dispatch::raw::Retained>::new(driver);
        let mut driver = root.narrow::<M::Access>();
        // SAFETY: the caller retains the installed owner selected for R;
        // rebinding preserves its slot and epoch and narrowing preserves access.
        unsafe { M::activate(owner, target, turn, &mut driver) };
    });
}

/// Statically selected activation capability for one exact route.
///
/// # Safety
/// `call` receives the exact installed owner selected for the target route.
pub unsafe trait Wakes<'d, M, R: route::App> {
    type Plan;

    const ROUTES: settings::Routes;

    fn select() -> Option<Self::Plan>;

    /// # Safety
    /// The call retains the exact pinned owner and plan selected for this route.
    unsafe fn call<'a, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'a,
            M,
            Self::Plan,
            executor::raw::Activate<'driver, 'turn, 'root, 'd>,
        >,
    ) where
        'd: 'driver + 'turn + 'root;
}

// SAFETY: projection preserves the exact pinned owner and selected inner plan.
unsafe impl<'d, M, R> Wakes<'d, M, R> for dispatch::raw::Projected<M>
where
    R: route::App,
    M: dispatch::raw::Projection<'d>,
    M::Inner: dispatch::raw::Manifold<'d, Route = R>,
{
    type Plan =
        <<M::Inner as dispatch::raw::Manifold<'d>>::Activate as Wakes<'d, M::Inner, R>>::Plan;

    const ROUTES: settings::Routes =
        <<M::Inner as dispatch::raw::Manifold<'d>>::Activate as Wakes<'d, M::Inner, R>>::ROUTES;

    fn select() -> Option<Self::Plan> {
        <<M::Inner as dispatch::raw::Manifold<'d>>::Activate as Wakes<'d, M::Inner, R>>::select()
    }

    unsafe fn call<'a, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'a,
            M,
            Self::Plan,
            executor::raw::Activate<'driver, 'turn, 'root, 'd>,
        >,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: projection keeps the installed owner pinned and preserves its selected plan.
        unsafe {
            <<M::Inner as dispatch::raw::Manifold<'d>>::Activate as Wakes<'d, M::Inner, R>>::call(
                call.map_owner(dispatch::raw::Projection::inner_mut),
            )
        }
    }
}

// SAFETY: the singleton route selects M's plain activation with the same pinned owner.
unsafe impl<'d, M, R> Wakes<'d, M, R> for dispatch::raw::Plain
where
    M: dispatch::raw::Activate<'d, R, Access = dispatch::raw::Plain>,
    R: route::App,
{
    type Plan = ();

    const ROUTES: settings::Routes = settings::Routes::singleton_id(R::ID);

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    unsafe fn call<'a, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'a,
            M,
            Self::Plan,
            executor::raw::Activate<'driver, 'turn, 'root, 'd>,
        >,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: this call retains the exact installed owner selected for R.
        unsafe { activate::<M, R>(call) };
    }
}

// SAFETY: the singleton route selects M's retained activation with the same pinned owner.
unsafe impl<'d, M, R> Wakes<'d, M, R> for dispatch::raw::Retained
where
    M: dispatch::raw::Activate<'d, R, Access = dispatch::raw::Retained>,
    R: route::App,
{
    type Plan = ();

    const ROUTES: settings::Routes = settings::Routes::singleton_id(R::ID);

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    unsafe fn call<'a, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'a,
            M,
            Self::Plan,
            executor::raw::Activate<'driver, 'turn, 'root, 'd>,
        >,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        // SAFETY: this call retains the exact installed owner selected for R.
        unsafe { activate::<M, R>(call) };
    }
}
