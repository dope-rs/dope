use std::{ops, pin};

use dope_core::{
    driver::{schedule, settings},
    io,
};

use crate::executor;

/// Application storage metadata independent of lifecycle eligibility.
pub trait Layout<'d> {
    /// Pre-park state selected once and retained with this pinned application.
    type ParkPlan;
}

/// # Safety
/// Visit each owner once through unchanged pinned projections, presence, and operation authority.
/// Admission includes every present owner's route, without merging duplicate owners.
/// Keep the root pinned; shutdown preserves client storage and finish destroys dependents before providers.
pub unsafe trait App<'d>: Sized + Layout<'d> {
    const ROUTES: settings::Routes;

    /// Routes whose installed owners may receive activation.
    const WAKE_ROUTES: settings::Routes = settings::Routes::EMPTY;

    /// Upper bound on idle coordinator probes, including delegated probes.
    const COORDS: usize;

    fn select_park() -> Option<Self::ParkPlan>;

    /// Prepares the selected coordinator shares once after installation.
    /// The count does not exceed `COORDS`; field presence and shares remain
    /// stable through finish. `None` reports count overflow.
    fn prepare_park(_: pin::Pin<&Self>, _: &mut Self::ParkPlan) -> Option<executor::raw::Count> {
        Some(executor::raw::Count::new(Self::COORDS))
    }

    /// # Safety
    /// The exact pinned root is installed and this plan completed preparation
    /// and admission for that root. The park authority belongs to the same root.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: pin::Pin<&'app mut Self>,
        plan: &mut Self::ParkPlan,
        park: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root;

    /// Accumulates the routes of the owners actually present in this pinned root.
    fn admit(
        _: pin::Pin<&Self>,
        needs: &mut settings::claim::Needs,
    ) -> Result<(), settings::errors::Claim> {
        *needs = needs.include(settings::claim::Claim::new(Self::ROUTES))?;
        Ok(())
    }

    fn released(released: executor::raw::Released<'_, Self>) -> executor::raw::Released<'_, Self> {
        released
    }

    fn install<'app, 'owner>(
        install: executor::raw::Install<'app, 'owner, 'd, Self>,
    ) -> executor::raw::Install<'app, 'owner, 'd, Self> {
        install
    }

    /// Returns whether a present owner handled this activation. On `false`,
    /// the caller may continue through the remaining owners of the same root.
    ///
    /// # Safety
    /// The activation retains this exact installed pinned root and a target
    /// declared by `WAKE_ROUTES`.
    unsafe fn activate<'app, 'driver, 'turn, 'root>(
        _: pin::Pin<&'app mut Self>,
        _: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn + 'root,
    {
        false
    }

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Dispatch<'app, 'driver, 'turn, 'root, 'd, Self>,
    ) -> ops::ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root,
    {
        ops::ControlFlow::Break(call.into_event())
    }

    fn progress(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d>;

    fn stopping(
        app: pin::Pin<&Self>,
        view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d>;

    fn shutdown<'app, 'turn, 'driver, 'owner>(
        shutdown: executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>,
    ) -> executor::raw::Shutdown<'app, 'turn, 'driver, 'owner, 'd, Self>
    where
        'd: 'owner,
    {
        shutdown
    }

    fn finish<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        finish
    }
}
