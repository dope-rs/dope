use std::{cell::Cell, pin::Pin, task};

use dope_core::driver::schedule;
use dope_manifold::{
    app::dispatch::raw,
    client::attempt::{self, queue},
};
use dope_net::tcp::Tcp;
use dope_runtime::executor::root;

pub mod empty;

pub struct Cancellation<'source, 'd> {
    pub(crate) lease: Option<queue::Lease<'source, 'd, Tcp>>,
}

#[derive(Debug)]
pub struct Completed;

impl<'d> root::Root<'d> for Cancellation<'_, 'd> {
    type Output = Result<attempt::Id<'d>, Completed>;

    fn poll(context: root::Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, _, _, _) = context.into_parts();
        let Some(lease) = root.get_mut().lease.take() else {
            return task::Poll::Ready(Err(Completed));
        };
        let key = lease.id();
        lease.cancel();
        task::Poll::Ready(Ok(key))
    }
}

#[pin_project::pin_project]
pub struct Exhausted<'a, M> {
    #[pin]
    pub(crate) inner: M,
    pub(crate) exhaust: &'a Cell<bool>,
}

impl<'d, M> raw::Projection<'d> for Exhausted<'_, M> {
    type Inner = M;

    fn inner(self: Pin<&Self>) -> Pin<&M> {
        self.project_ref().inner
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut M> {
        self.project().inner
    }
}

// SAFETY: each operation retains the same pinned owner and all of its loans.
unsafe impl<'d, M: raw::Manifold<'d>> raw::Manifold<'d> for Exhausted<'_, M> {
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
        if self.as_ref().project_ref().exhaust.replace(false) {
            turn.reborrow().maint().share::<{ schedule::LIMIT }, _>(|| {
                while turn.maint().permit().is_some() {}
                // SAFETY: quota exhaustion changes no owner or route proof.
                unsafe { M::pre_park(self.as_mut().project().inner, turn, driver) };
            });
        } else {
            // SAFETY: projection retains the exact installed owner and its loans.
            unsafe { M::pre_park(self.as_mut().project().inner, turn, driver) };
        }
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut raw::Context<'_, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self> {
        M::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}
