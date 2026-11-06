use std::pin::Pin;

use dope_core::driver::schedule;
use dope_manifold::app::dispatch::raw;

#[pin_project::pin_project]
pub struct Limited<M> {
    #[pin]
    pub(crate) inner: M,
}

impl<'d, M> raw::Projection<'d> for Limited<M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().inner
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().inner
    }
}

// SAFETY: every lifecycle operation retains the same installed, pinned owner.
// The shared quota changes only its available maintenance work for this call.
unsafe impl<'d, M: raw::Manifold<'d>> raw::Manifold<'d> for Limited<M> {
    type Route = M::Route;
    type Dispatch = raw::Projected<Self>;
    type Install = raw::Projected<Self>;
    type Finish = raw::Projected<Self>;
    type Activate = raw::Projected<Self>;
    type Probe = raw::Projected<Self>;
    type PrePark = M::PrePark;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self> {
        turn.reborrow().maint().share::<{ schedule::LIMIT }, _>(|| {
            // SAFETY: projection retains the exact installed owner and all loans.
            unsafe { M::pre_park(self.as_mut().project().inner, turn, driver) };
        });
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self> {
        turn.reborrow().maint().share::<{ schedule::LIMIT }, _>(|| {
            M::shutdown(self.as_mut().project().inner, turn, driver);
        });
        self
    }
}

// SAFETY: the wrapper forwards complete shutdown, quiescence and finalization.
unsafe impl<'d, M: raw::Resume<'d>> raw::Resume<'d> for Limited<M> {}
