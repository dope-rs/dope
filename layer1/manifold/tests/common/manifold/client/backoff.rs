use std::{cell::Cell, pin::Pin};

use dope_core::driver::{route, schedule, schedule::clock};
use dope_manifold::app::dispatch::raw;
use dope_test::harness::signals::{Gate, Progress as _};

#[pin_project::pin_project]
pub struct Exhausted<'a, 'd, M> {
    #[pin]
    pub(crate) inner: M,
    pub(crate) closed: &'a Gate,
    pub(crate) injected: &'a Cell<bool>,
    pub(crate) due: Option<clock::Deadline<'d>>,
}

fn exhaust<'d>(
    due: Option<clock::Deadline<'d>>,
    injected: &Cell<bool>,
    turn: schedule::Turn<'_, 'd>,
    mut run: impl FnMut(),
) {
    if due.is_some_and(|at| at <= turn.clock().now()) && !injected.replace(true) {
        turn.maint().share::<{ schedule::LIMIT }, _>(|| {
            while turn.maint().permit().is_some() {}
            // Visit every service phase without a permit, including Submission.
            for _ in 0..4 {
                run();
            }
        });
        return;
    }
    run();
}

impl<'d, M> raw::Projection<'d> for Exhausted<'_, 'd, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().inner
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().inner
    }
}

// SAFETY: every operation retains the same installed, pinned connector and engine.
unsafe impl<'d, M> raw::Manifold<'d> for Exhausted<'_, 'd, M>
where
    M: raw::Manifold<'d, PrePark = raw::Retained> + raw::Projection<'d>,
    M::Inner: raw::Activate<'d, M::Route, Access = raw::Retained>,
{
    type Route = M::Route;
    type Dispatch = raw::Projected<Self>;
    type Install = raw::Projected<Self>;
    type Finish = raw::Projected<Self>;
    type Activate = raw::Retained;
    type Probe = raw::Projected<Self>;
    type PrePark = raw::Retained;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let mut this = self.as_mut().project();
        exhaust(*this.due, this.injected, turn.reborrow(), || {
            // SAFETY: projection retains the exact installed connector and its loans.
            unsafe { M::pre_park(this.inner.as_mut(), turn.reborrow(), driver) };
        });
        if this.closed.hits() == 1 && this.due.is_none() && !this.injected.get() {
            *this.due = driver.driver_ref().timer().earliest();
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        M::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}

// SAFETY: activation retains the canonical route and the same pinned engine.
unsafe impl<'d, M> raw::Activate<'d, M::Route> for Exhausted<'_, 'd, M>
where
    M: raw::Manifold<'d, PrePark = raw::Retained> + raw::Projection<'d>,
    M::Inner: raw::Activate<'d, M::Route, Access = raw::Retained>,
{
    type Access = raw::Retained;

    unsafe fn activate<'turn>(
        self: Pin<&mut Self>,
        target: route::Target<'d, M::Route>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        let mut this = self.project();
        exhaust(*this.due, this.injected, turn.reborrow(), || {
            // SAFETY: projection preserves the canonical target and exact installed engine.
            unsafe {
                M::Inner::activate(
                    this.inner.as_mut().inner_mut(),
                    target,
                    turn.reborrow(),
                    driver,
                );
            }
        });
    }
}
