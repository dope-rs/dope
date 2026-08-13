use core::{marker, pin};
use std::task;

use dope::core::driver::{
    self, retained,
    schedule::{
        self,
        ready::{self, completion},
    },
};
use o3::{cell::region, collections::batch::set};

use crate::abi;

/// Exact, single-use admission for polling one pinned fiber.
/// It is constructed only after consuming one application-work credit.
pub struct PollCall<'call, 'turn, 'd: 'turn, F> {
    fiber: pin::Pin<&'call mut F>,
    context: pin::Pin<&'call mut Context<'turn, 'd>>,
    _permit: schedule::ApplicationPermit<'turn, 'd>,
}

impl<'call, 'turn, 'd: 'turn, F> PollCall<'call, 'turn, 'd, F> {
    fn new(
        fiber: pin::Pin<&'call mut F>,
        context: pin::Pin<&'call mut Context<'turn, 'd>>,
        permit: schedule::ApplicationPermit<'turn, 'd>,
    ) -> Self {
        Self {
            fiber,
            context,
            _permit: permit,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        pin::Pin<&'call mut F>,
        pin::Pin<&'call mut Context<'turn, 'd>>,
    ) {
        (self.fiber, self.context)
    }
}

#[pin_project::pin_project]
pub struct Context<'poll, 'd: 'poll> {
    wake: Waker<'d>,
    root: RootWaker<'d>,
    work: schedule::Application<'poll, 'd>,
    driver: retained::Context<'poll, 'poll, 'd>,
    #[pin]
    _pin: marker::PhantomPinned,
}

impl<'poll, 'd: 'poll> Context<'poll, 'd> {
    pub(crate) fn from_waker(
        wake: Waker<'d>,
        root: RootWaker<'d>,
        work: schedule::Application<'poll, 'd>,
        driver: retained::Context<'poll, 'poll, 'd>,
    ) -> Self {
        Self {
            wake,
            root,
            work,
            driver,
            _pin: marker::PhantomPinned,
        }
    }

    #[doc(hidden)]
    pub fn from_target(
        target: ready::Target<'d>,
        work: schedule::Application<'poll, 'd>,
        driver: retained::Context<'poll, 'poll, 'd>,
    ) -> Self {
        let root = RootWaker::from(target);
        Self::from_waker(root.into(), root, work, driver)
    }

    pub fn waker(&self) -> WakerRef<'_, 'd> {
        WakerRef::new(&self.wake)
    }

    /// Returns the exact driver-owned root carried through this poll tree.
    /// It can own a persistent scheduler whose members outlive child adapters.
    pub fn root_waker(self: pin::Pin<&Self>) -> RootWaker<'d> {
        self.get_ref().root
    }

    #[doc(hidden)]
    pub fn completion_waker(self: pin::Pin<&Self>) -> completion::Waker<'d> {
        self.get_ref().wake.completion()
    }

    pub fn driver_access(self: pin::Pin<&mut Self>) -> retained::Context<'_, '_, 'd> {
        self.project().driver.reborrow()
    }

    /// Polls one exact child after consuming one credit from this turn.
    /// `None` means the application budget is exhausted. The current wake
    /// target is reactivated before returning so the child remains runnable.
    pub fn try_poll<'call, F>(
        self: pin::Pin<&'call mut Self>,
        fiber: pin::Pin<&'call mut F>,
    ) -> Option<task::Poll<F::Output>>
    where
        F: abi::Fiber<'d>,
    {
        let Some(permit) = self.as_ref().get_ref().work.permit() else {
            self.as_ref().get_ref().wake();
            return None;
        };
        Some(self.poll_admitted(fiber, permit))
    }

    pub(crate) fn poll_admitted<'call, F>(
        self: pin::Pin<&'call mut Self>,
        fiber: pin::Pin<&'call mut F>,
        permit: schedule::ApplicationPermit<'poll, 'd>,
    ) -> task::Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        F::poll(PollCall::new(fiber, self, permit))
    }

    pub(crate) fn admit_next<I: set::DenseIndex>(
        self: pin::Pin<&Self>,
        ready: &mut set::Drain<'_, I>,
    ) -> schedule::ApplicationAdmission<'poll, 'd, I> {
        self.get_ref().work.admit_next(ready)
    }

    pub(crate) fn admit(self: pin::Pin<&Self>) -> Option<schedule::ApplicationPermit<'poll, 'd>> {
        self.get_ref().work.permit()
    }

    pub(crate) fn poll_admitted_with_waker<F>(
        mut self: pin::Pin<&mut Self>,
        fiber: pin::Pin<&mut F>,
        wake: Waker<'d>,
        permit: schedule::ApplicationPermit<'poll, 'd>,
    ) -> task::Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        let this = self.as_mut().project();
        let mut child = pin::pin!(Context {
            wake,
            root: *this.root,
            work: *this.work,
            driver: this.driver.reborrow(),
            _pin: marker::PhantomPinned,
        });
        child.as_mut().poll_admitted(fiber, permit)
    }

    pub fn region_token(self: pin::Pin<&mut Self>) -> &mut region::Token<'d> {
        self.project().driver.region_token()
    }

    pub fn wake(&self) {
        self.wake.wake();
    }
}

/// Driver-owned wake target that cannot name a task node.
/// Only the root boundary supplies it; child contexts cannot recover one.
/// Generational ready keys make wakes after slot release a no-op.
#[derive(Clone, Copy)]
pub struct RootWaker<'d> {
    target: ready::Target<'d>,
}

impl<'d> RootWaker<'d> {
    pub fn wake(self) {
        self.target.wake();
    }
}

impl<'d> From<ready::Target<'d>> for RootWaker<'d> {
    fn from(target: ready::Target<'d>) -> Self {
        Self { target }
    }
}

impl<'d> From<RootWaker<'d>> for Waker<'d> {
    fn from(root: RootWaker<'d>) -> Self {
        Waker::from_wake(completion::Wake::from(root.target))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Waker<'d>(pub(crate) completion::Wake<'d>);

impl<'d> Waker<'d> {
    pub(crate) fn from_wake(wake: completion::Wake<'d>) -> Self {
        Self(wake)
    }

    pub fn from_ready(driver: driver::Reference<'d>, key: ready::Key<'d>) -> Self {
        Self(completion::Wake::from_ready(driver, key))
    }

    pub(crate) fn completion(self) -> completion::Waker<'d> {
        self.0.completion()
    }

    pub fn wake(self) {
        self.0.wake();
    }
}

pub struct WakerRef<'a, 'd> {
    waker: &'a Waker<'d>,
}

impl<'a, 'd> WakerRef<'a, 'd> {
    fn new(waker: &'a Waker<'d>) -> Self {
        Self { waker }
    }

    pub fn wake(&self) {
        self.waker.wake();
    }
}
