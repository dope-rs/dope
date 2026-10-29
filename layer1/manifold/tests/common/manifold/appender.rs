use std::{cell::Cell, pin::Pin};

use dope_core::driver::schedule;
use dope_manifold::app::dispatch;

/// Observes the real appender immediately after submission, before a completion
/// can retire its first block and unblock the queued second block.
#[pin_project::pin_project]
pub struct Observed<'a, M> {
    #[pin]
    pub(crate) writer: M,
    pub(crate) checked: &'a Cell<Option<bool>>,
}

impl<'d, M: dispatch::raw::Manifold<'d>> dispatch::raw::Projection<'d> for Observed<'_, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().writer
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().writer
    }
}

// SAFETY: every operation projects the same installed, pinned writer. The
// observation only queries its existing liveness capability after submission.
unsafe impl<'d, M> dispatch::raw::Manifold<'d> for Observed<'_, M>
where
    M: dispatch::raw::Manifold<'d, PrePark = dispatch::raw::Retained>,
{
    type Route = M::Route;
    type Dispatch = dispatch::raw::Projected<Self>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = M::Shutdown;

    const COORDS: usize = M::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        // SAFETY: projection retains the exact installed writer and its loans.
        unsafe { M::pre_park(self.as_mut().project().writer, turn, driver) };
        let this = self.as_ref().project_ref();
        if this.checked.get().is_none() {
            let waiting = matches!(
                <M::Probe as dispatch::raw::Probe<'d, M>>::progress(
                    this.writer,
                    driver.regions(),
                    driver.driver_ref().clock(),
                ),
                schedule::Progress::Waiting(_)
            );
            this.checked.set(Some(waiting));
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        M::shutdown(self.as_mut().project().writer, turn, driver);
        self
    }
}
