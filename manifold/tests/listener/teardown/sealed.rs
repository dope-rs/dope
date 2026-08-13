use std::{ops, pin::Pin};

use dope_core::driver::schedule::Progress;
use dope_manifold::dispatch::typed::Token;

use crate::teardown::DropLive;

// SAFETY: while live, `DropLive` preserves `M`'s route and delegates every event to the
// same pinned value; once stopping, it rejects further entry before teardown.
unsafe impl<'d, M> dope_manifold::dispatch::raw::Manifold<'d> for DropLive<M>
where
    M: dope_manifold::dispatch::raw::Manifold<'d>,
{
    const ID: u8 = M::ID;
    type Dispatch = M::Dispatch;
    type Activate = M::Activate;
    type PrePark = M::PrePark;
    type Shutdown = dope_manifold::dispatch::raw::Plain;

    fn install(self: Pin<&mut Self>, install: &mut dope_core::driver::lifecycle::Install<'_, 'd>) {
        M::install(self.project().inner, install);
    }

    unsafe fn dispatch<'turn>(
        self: Pin<&mut Self>,
        ev: dope_core::io::Event<'d>,
        turn: dope_core::driver::schedule::Turn<'turn, 'd>,
        driver: &mut dope_manifold::dispatch::raw::Context<'_, '_, 'd, Self::Dispatch>,
    ) -> ops::ControlFlow<dope_core::io::Event<'d>> {
        let this = self.project();
        if !*this.stopping {
            unsafe { M::dispatch(this.inner, ev, turn, driver) }
        } else {
            ops::ControlFlow::Continue(())
        }
    }

    unsafe fn activate<'turn>(
        self: Pin<&mut Self>,
        target: Token<'d, Self>,
        turn: dope_core::driver::schedule::Turn<'turn, 'd>,
        driver: &mut dope_manifold::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let this = self.project();
        if !*this.stopping {
            let target = target.retag::<M>();
            unsafe { M::activate(this.inner, target, turn, driver) };
        }
    }

    unsafe fn pre_park<'turn>(
        self: Pin<&mut Self>,
        turn: dope_core::driver::schedule::Turn<'turn, 'd>,
        driver: &mut dope_manifold::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let this = self.project();
        if !*this.stopping {
            unsafe { M::pre_park(this.inner, turn, driver) };
        }
    }

    fn progress(self: Pin<&Self>, region: &o3::cell::region::Token<'d>) -> Progress<'d> {
        let this = self.project_ref();
        if *this.stopping {
            Progress::Quiescent
        } else {
            M::progress(this.inner, region)
        }
    }

    fn shutdown<'turn>(
        self: Pin<&mut Self>,
        _turn: dope_core::driver::schedule::Turn<'turn, 'd>,
        _driver: &mut dope_manifold::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        *self.project().stopping = true;
    }
}
