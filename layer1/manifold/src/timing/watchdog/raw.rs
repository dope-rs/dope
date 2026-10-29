use std::{pin, task};

use dope_core::driver::{route, route::Identity, schedule};

use crate::{app::dispatch, timing::watchdog::Watchdog};

// SAFETY: the pinned timer registration and kernel timer remain owned by the
// exact installed application through shutdown and finalization.
unsafe impl<'d, ID: Identity> dispatch::raw::Manifold<'d> for Watchdog<'d, ID> {
    const COORDS: usize = 0;

    type Route = ID;
    type Dispatch = dispatch::raw::Omit;
    type Install = dispatch::raw::Plain;
    type Finish = Self;
    type Activate = dispatch::raw::Plain;
    type Probe = Self;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: pin::Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
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
        let this = self.as_mut().project();
        this.route.as_ref().cancel();
        *this.next = None;
        self
    }
}

// SAFETY: unit selection binds every query to this exact pinned watchdog.
unsafe impl<'d, ID: Identity> dispatch::raw::Probe<'d, Self> for Watchdog<'d, ID> {
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        match *call.owner().project_ref().next {
            Some(deadline) => schedule::Progress::until(deadline),
            None => schedule::Progress::Quiescent,
        }
    }
}

// SAFETY: the pinned timer registration and kernel timer remain owned by the
// exact installed application through every activation.
unsafe impl<'d, ID: Identity> dispatch::raw::Activate<'d, ID> for Watchdog<'d, ID> {
    type Access = dispatch::raw::Plain;

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _: route::Target<'d, ID>,
        _: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        let this = self.project();
        let Some(deadline) = this.next else {
            return;
        };
        let now = driver.driver_ref().clock().now();
        if this.route.as_ref().poll(now) == task::Poll::Ready(()) {
            this.kernel.refresh(driver.reborrow());
        } else if this.route.as_ref().is_armed() {
            return;
        }
        let next = now.clamp_add(*this.period);
        *deadline = next;
        this.route.as_ref().arm(next);
    }
}
