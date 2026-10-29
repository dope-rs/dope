use std::{cell::Cell, rc::Rc, task::Poll};

use dope::{
    core::driver::{
        route::{Epoch, KeyTag, indices::SlotIndex},
        schedule,
        schedule::{Progress, ready::wake::store},
        settings::{Profile, QueueLayout, Routes, Schedule},
    },
    manifold::timing::Policy,
};
use dope_fiber::{abi::Fiber, context};
use o3::base::time::Positive;

use crate::common::runtime::profile::Tasks;

pub struct Domain;

pub struct PendingTask;

impl<'d> Fiber<'d> for PendingTask {
    type Output = ();

    fn poll(_: context::PollCall<'_, '_, 'd, Self>) -> Poll<()> {
        Poll::Pending
    }
}

pub struct WakeProfile;

impl Profile for WakeProfile {
    const QUEUES: QueueLayout = Tasks::QUEUES;
    const SCHEDULER: Schedule = Schedule::fixed::<3, 2>();
}

impl Policy for WakeProfile {
    const CONNECT_DEADLINE: Positive = Positive::from_millis::<1>();
    const IDLE_WINDOW: Positive = Positive::from_millis::<1>();
    const SEND_DEADLINE: Positive = Positive::from_millis::<1>();
}

pub struct WakeState {
    pub(crate) allowed: Cell<bool>,
    pub(crate) polls: Cell<u32>,
    pub(crate) ticks: Cell<u32>,
    pub(crate) foreign: Cell<u32>,
    pub(crate) matched: Cell<bool>,
    pub(crate) polls_before_wake: Cell<Option<u32>>,
}

pub struct WakeFiber<'target, 'd> {
    pub(crate) state: &'target WakeState,
    pub(crate) wake: &'target store::Slot<'d>,
}

impl<'d> Fiber<'d> for WakeFiber<'_, 'd> {
    type Output = (u32, u32);

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        let state = this.state;
        state.polls.set(state.polls.get() + 1);
        if state.allowed.get() {
            return Poll::Ready((state.polls.get(), state.foreign.get()));
        }
        this.wake.set(context.as_ref().completion());
        Poll::Pending
    }
}

pub struct Selected;

pub struct WakeProbe<'slot, 'd> {
    pub(crate) wake: &'slot store::Slot<'d>,
    pub(crate) state: Rc<WakeState>,
}

use std::{ops::ControlFlow, pin::Pin};

use dope::{
    core::{driver::settings, io},
    runtime::executor,
};

impl<'d> executor::raw::Layout<'d> for WakeProbe<'_, 'd> {
    type ParkPlan = Selected;
}

// SAFETY: the application owns only an Rc and a borrowed wake slot, both Unpin.
unsafe impl<'d> executor::raw::App<'d> for WakeProbe<'_, 'd> {
    const ROUTES: settings::Routes = Routes::singleton::<KeyTag<0>>();
    const WAKE_ROUTES: settings::Routes = Self::ROUTES;
    fn dispatch<'app, 'driver, 'turn, 'root>(
        _: executor::raw::Dispatch<'app, 'driver, 'turn, 'root, 'd, Self>,
    ) -> ControlFlow<io::Event<'d>>
    where
        'd: 'root,
        Self: 'root,
    {
        ControlFlow::Continue(())
    }
    fn progress(app: Pin<&Self>, _: executor::raw::Progress<'_, '_, 'd>) -> schedule::Progress<'d> {
        if app.state.allowed.get() {
            Progress::Quiescent
        } else {
            Progress::Runnable
        }
    }
    fn stopping(
        app: Pin<&Self>,
        _view: executor::raw::Progress<'_, '_, 'd>,
    ) -> schedule::Progress<'d> {
        if app.state.allowed.get() {
            schedule::Progress::Quiescent
        } else {
            schedule::Progress::Runnable
        }
    }
    const COORDS: usize = 0;
    fn select_park() -> Option<Selected> {
        Some(Selected)
    }
    /// # Safety
    /// The root, prepared plan, and park authority belong to this installed owner.
    unsafe fn park<'app, 'driver, 'turn, 'root>(
        app: Pin<&'app mut Self>,
        Selected: &mut Self::ParkPlan,
        _: executor::raw::Park<'driver, 'turn, 'root, 'd>,
    ) where
        'd: 'driver + 'turn + 'root,
    {
        if app.state.polls.get() == 0 || app.state.allowed.get() {
            return;
        }
        app.state.ticks.set(app.state.ticks.get() + 1);
        if app.state.ticks.get() == 3 {
            app.state.polls_before_wake.set(Some(app.state.polls.get()));
            app.state.allowed.set(true);
            app.wake.wake_once();
        }
    }

    /// # Safety
    /// The runtime supplies this route-zero application and its exact retained driver.
    unsafe fn activate<'app, 'driver, 'turn, 'root>(
        app: Pin<&'app mut Self>,
        event: executor::raw::Activate<'driver, 'turn, 'root, 'd>,
    ) -> bool
    where
        'd: 'driver + 'turn + 'root,
    {
        let (target, driver) = event.into_driver();
        let expected = driver
            .driver_ref()
            .targets::<KeyTag<0>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL)
            .dispatch();
        app.state
            .matched
            .set(app.state.matched.get() && expected.matches(target));
        app.state.foreign.set(app.state.foreign.get() + 1);
        true
    }
}
