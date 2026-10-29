use std::{convert, pin};

use dope_core::driver::{schedule, schedule::clock};
use o3::base::cell::region;

use crate::app::dispatch;

/// Statically selects exact-owner liveness query plans.
/// # Safety
/// Plans and `Quiescent` must describe the selected owner's complete live work.
pub unsafe trait Probe<'d, M = Self>: Sized {
    type Plan;

    fn select() -> Option<Self::Plan>;

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, M, Self::Plan>) -> schedule::Progress<'d>;

    fn stop(call: dispatch::raw::Call<'_, '_, '_, 'd, M, Self::Plan>) -> schedule::Progress<'d> {
        Self::run(call)
    }

    fn progress<'a, 'region, 'turn>(
        owner: pin::Pin<&'a M>,
        region: &'region (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        clock: clock::Clock<'turn, 'd>,
    ) -> schedule::Progress<'d> {
        let Some(plan) = Self::select() else {
            return schedule::Progress::Quiescent;
        };
        Self::run(dispatch::raw::Call::new(owner, plan, region, clock))
    }

    fn stopping<'a, 'region, 'turn>(
        owner: pin::Pin<&'a M>,
        region: &'region (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
        clock: clock::Clock<'turn, 'd>,
    ) -> schedule::Progress<'d> {
        let Some(plan) = Self::select() else {
            return schedule::Progress::Quiescent;
        };
        Self::stop(dispatch::raw::Call::new(owner, plan, region, clock))
    }
}

// SAFETY: no plan can be selected, so no owner query can be constructed.
unsafe impl<'d, M> Probe<'d, M> for dispatch::raw::Omit {
    type Plan = convert::Infallible;

    fn select() -> Option<Self::Plan> {
        None
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, M, Self::Plan>) -> schedule::Progress<'d> {
        call.into_never()
    }
}

// SAFETY: projection preserves the exact pinned owner and selected inner plan.
unsafe impl<'d, M> Probe<'d, M> for dispatch::raw::Projected<M>
where
    M: dispatch::raw::Projection<'d>,
    M::Inner: dispatch::raw::Manifold<'d>,
{
    type Plan = <<<M as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::Probe as Probe<
        'd,
        <M as dispatch::raw::Projection<'d>>::Inner,
    >>::Plan;

    fn select() -> Option<Self::Plan> {
        <<<M as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::Probe as Probe<
            'd,
            <M as dispatch::raw::Projection<'d>>::Inner,
        >>::select()
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, M, Self::Plan>) -> schedule::Progress<'d> {
        <<M::Inner as dispatch::raw::Manifold<'d>>::Probe as Probe<'d, M::Inner>>::run(
            call.map_owner(dispatch::raw::Projection::inner),
        )
    }

    fn stop(call: dispatch::raw::Call<'_, '_, '_, 'd, M, Self::Plan>) -> schedule::Progress<'d> {
        <<M::Inner as dispatch::raw::Manifold<'d>>::Probe as Probe<'d, M::Inner>>::stop(
            call.map_owner(dispatch::raw::Projection::inner),
        )
    }
}
