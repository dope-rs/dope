use std::{pin, task};

use dope_core::driver::{route, route::Identity, schedule};

use crate::{
    app::dispatch,
    timing::interval::{Cadence, Control, Interval, PERIOD, Tick},
};

// SAFETY: Control only consumes a coalesced tick and keeps the owner pinned.
unsafe impl<'d, ID: Identity> dispatch::raw::Control<'d> for Interval<'d, ID> {
    type View<'step>
        = Control<'step, 'd, ID>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        Control { inner: owner }
    }
}

// SAFETY: Registration owns all timer linkage and ready is generation checked.
unsafe impl<'d, ID: Identity> dispatch::raw::Manifold<'d> for Interval<'d, ID> {
    const COORDS: usize = 0;

    type Route = ID;
    type Dispatch = dispatch::raw::Omit;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Plain;
    type Probe = Self;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: pin::Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self>
    where
        Self: 'root,
    {
        let this = self.as_mut().project();
        if this.route.as_ref().is_armed() {
            return self;
        }
        let deadline = match *this.cadence {
            Cadence::Next(deadline) => deadline,
            Cadence::Starting => {
                let now = driver.driver_ref().clock().now();
                let deadline = now.clamp_add(PERIOD);
                if deadline <= now {
                    *this.cadence = Cadence::Stopped;
                    return self;
                }
                *this.cadence = Cadence::Next(deadline);
                deadline
            }
            Cadence::Stopped => return self,
        };
        this.route.as_ref().arm(deadline);
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
        *this.tick = Tick::Empty;
        *this.cadence = Cadence::Stopped;
        self
    }
}

// SAFETY: shutdown cancels the timer registration and clears the tick before
// reporting quiescence; no retained native operation belongs to the interval.
unsafe impl<'d, ID: Identity> dispatch::raw::Resume<'d> for Interval<'d, ID> {}

// SAFETY: unit selection binds every query to this exact pinned interval.
unsafe impl<'d, ID: Identity> dispatch::raw::Probe<'d, Self> for Interval<'d, ID> {
    type Plan = ();

    fn select() -> Option<Self::Plan> {
        Some(())
    }

    fn run(call: dispatch::raw::Call<'_, '_, '_, 'd, Self, ()>) -> schedule::Progress<'d> {
        match *call.owner().project_ref().cadence {
            Cadence::Next(deadline) => schedule::Progress::until(deadline),
            Cadence::Starting | Cadence::Stopped => schedule::Progress::Quiescent,
        }
    }
}

// SAFETY: Registration owns all timer linkage and ready is generation checked.
unsafe impl<'d, ID: Identity> dispatch::raw::Activate<'d, ID> for Interval<'d, ID> {
    type Access = dispatch::raw::Plain;

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _: route::Target<'d, ID>,
        _: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, '_, 'd, Self::Access>,
    ) {
        let this = self.project();
        let Cadence::Next(previous) = *this.cadence else {
            return;
        };
        let now = driver.driver_ref().clock().now();
        if this.route.as_ref().poll(now) == task::Poll::Ready(()) {
            *this.tick = Tick::Ready;
            *this.cadence = Self::next(previous, now).map_or(Cadence::Stopped, Cadence::Next);
        }
    }
}
