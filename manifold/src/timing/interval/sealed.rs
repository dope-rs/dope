use std::{pin, task};

use dope_core::driver::schedule;
use o3::cell::region;

use crate::dispatch::typed;

// SAFETY: `Control` only consumes the coalesced tick. It cannot expose, move,
// replace, or drop the installed owner or its timer registration.
unsafe impl<'d, const ID: u8> crate::dispatch::raw::Controlled<'d> for super::Interval<'d, ID> {
    type Control<'step>
        = super::Control<'step, 'd, ID>
    where
        'd: 'step;

    unsafe fn control<'step>(self: pin::Pin<&'step mut Self>) -> Self::Control<'step>
    where
        'd: 'step,
    {
        super::Control { inner: self }
    }
}

// SAFETY: Registration owns all timer linkage and ready is generation checked.
unsafe impl<'d, const ID: u8> crate::dispatch::raw::Manifold<'d> for super::Interval<'d, ID> {
    const ID: u8 = ID;
    type Dispatch = crate::dispatch::raw::Plain;
    type Activate = crate::dispatch::raw::Plain;
    type PrePark = crate::dispatch::raw::Plain;
    type Shutdown = crate::dispatch::raw::Plain;

    unsafe fn activate<'turn>(
        self: pin::Pin<&mut Self>,
        _target: typed::Token<'d, Self>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Activate>,
    ) {
        let this = self.project();
        if *this.stopped {
            return;
        }
        let now = driver.turn_now();
        if this.route.as_ref().poll(
            driver.driver_ref().scheduler().deadline(now),
            driver.driver_ref(),
        ) == task::Poll::Ready(())
        {
            *this.tick = true;
            let next = match *this.next {
                Some(previous) => Self::following_deadline(previous, now),
                None => Self::deadline_after(now, super::PERIOD),
            };
            let Some(next) = next else {
                *this.next = None;
                *this.stopped = true;
                return;
            };
            *this.next = Some(next);
        }
    }

    unsafe fn pre_park<'turn>(
        self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::PrePark>,
    ) {
        let this = self.project();
        if *this.stopped || this.route.as_ref().is_armed() {
            return;
        }
        let deadline = match *this.next {
            Some(deadline) => deadline,
            None => {
                let now = driver.turn_now();
                let Some(deadline) = Self::deadline_after(now, super::PERIOD) else {
                    *this.stopped = true;
                    return;
                };
                *this.next = Some(deadline);
                deadline
            }
        };
        this.route.as_ref().arm(
            driver.driver_ref().scheduler().deadline(deadline),
            driver.driver_ref(),
        );
    }

    fn progress(self: pin::Pin<&Self>, region: &region::Token<'d>) -> schedule::Progress<'d> {
        match *self.project_ref().next {
            Some(deadline) => schedule::Progress::until(region, deadline),
            None => schedule::Progress::Quiescent,
        }
    }

    fn shutdown<'turn>(
        self: pin::Pin<&mut Self>,
        _turn: schedule::Turn<'turn, 'd>,
        _driver: &mut crate::dispatch::raw::Context<'_, '_, 'd, Self::Shutdown>,
    ) {
        let this = self.project();
        this.route.as_ref().cancel();
        *this.tick = false;
        *this.next = None;
        *this.stopped = true;
    }
}
