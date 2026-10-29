use std::pin;

use dope_core::driver::{
    route::{self, Identity, kind},
    schedule,
};

use crate::{
    app::{dispatch, dispatch::event},
    file::appender::{self, state::State},
};

pub(super) fn bytes<'a, const N: usize, const B: usize>(
    pending: &'a appender::Pending<'_, N, B>,
) -> &'a [u8] {
    // SAFETY: new binds length to this unchanged exclusive lease.
    // advance preserves remaining <= length; release consumes the owner.
    unsafe {
        pending
            .block()
            .bytes
            .get_unchecked(pending.written().into_raw()..)
    }
}

// SAFETY: Appender is installed pinned and owns its descriptor and fixed blocks,
// and flight slots through shutdown drain and finish.
unsafe impl<'p, 'd, ID: Identity, const N: usize, const B: usize> dispatch::raw::Manifold<'d>
    for appender::Appender<'p, 'd, ID, N, B>
{
    type Route = <ID as route::Materialize<kind::Write>>::With;
    type Dispatch = event::Driven<dispatch::raw::Retained>;
    type Install = dispatch::raw::Omit;
    type Finish = Self;
    type Activate = dispatch::raw::Omit;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize = 0;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.as_mut().get_mut();
        if let Some(pending) = this.prepare() {
            this.submit(pending, driver);
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self.as_mut().get_mut().close();
        self
    }
}

// SAFETY: unit selection binds every query to this exact pinned appender.
unsafe impl<'p, 'd, ID: Identity, const N: usize, const B: usize> dispatch::raw::Probe<'d, Self>
    for appender::Appender<'p, 'd, ID, N, B>
{
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        match call.owner().get_ref().progress() {
            State::Runnable => schedule::Progress::Runnable,
            State::Waiting => schedule::Progress::waiting(call.region()),
            State::Quiescent => schedule::Progress::Quiescent,
        }
    }
}

// SAFETY: Control keeps the owner pinned and only copies into idle blocks.
unsafe impl<'p, 'd, ID: Identity, const N: usize, const B: usize> dispatch::raw::Control<'d>
    for appender::Appender<'p, 'd, ID, N, B>
{
    type View<'step>
        = appender::Control<'step, 'p, 'd, ID, N, B>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        appender::Control { inner: owner }
    }
}
