use std::{pin, task};

use dope_core::driver::{
    self,
    schedule::{app, ready},
};

/// Runtime poll capability tying one exact root and driver to a poll.
pub struct Context<'poll, 'd: 'poll, R> {
    root: pin::Pin<&'poll mut R>,
    wake: ready::Target<'d>,
    work: app::Work<'poll, 'd>,
    driver: driver::Context<'poll, 'd>,
}

/// Remaining application-lane credits for one root poll.
#[derive(Clone, Copy)]
pub struct Budget<'poll, 'd> {
    wake: ready::Target<'d>,
    work: app::Work<'poll, 'd>,
}

impl<'poll, 'd: 'poll, R> Context<'poll, 'd, R> {
    pub(crate) fn new(
        root: pin::Pin<&'poll mut R>,
        wake: ready::Target<'d>,
        work: app::Work<'poll, 'd>,
        driver: driver::Context<'poll, 'd>,
    ) -> Self {
        Self {
            root,
            wake,
            work,
            driver,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        pin::Pin<&'poll mut R>,
        ready::Target<'d>,
        app::Work<'poll, 'd>,
        driver::Context<'poll, 'd>,
    ) {
        (self.root, self.wake, self.work, self.driver)
    }

    pub fn into_poll(
        self,
    ) -> (
        pin::Pin<&'poll mut R>,
        Budget<'poll, 'd>,
        driver::Context<'poll, 'd>,
    ) {
        (self.root, Budget::new(self.wake, self.work), self.driver)
    }
}

impl<'poll, 'd> Budget<'poll, 'd> {
    pub(crate) const fn new(wake: ready::Target<'d>, work: app::Work<'poll, 'd>) -> Self {
        Self { wake, work }
    }

    pub fn target(self) -> ready::Target<'d> {
        self.wake
    }

    /// Reserves one more application transition, requeueing the root when its
    /// current application-lane share is exhausted.
    pub fn try_continue(self) -> Option<app::Permit<'poll, 'd>> {
        let permit = self.work.permit();
        if permit.is_none() {
            self.wake.wake();
        }
        permit
    }
}

/// Transient pinned control-flow root.
/// Driver authority retains the application through terminal quiescence.
pub trait Root<'d>: Sized {
    type Output;
    fn poll(context: Context<'_, 'd, Self>) -> task::Poll<Self::Output>;
}

impl<'d, R: Root<'d>> Root<'d> for o3::mem::slot::Occupied<'_, R> {
    type Output = R::Output;

    fn poll(context: Context<'_, 'd, Self>) -> task::Poll<Self::Output> {
        let (root, wake, work, driver) = context.into_parts();
        // SAFETY: the executor owns this slot through every poll and its drop;
        // the inner root stays at this address until dropped before quiescence.
        let root = unsafe { pin::Pin::new_unchecked(root.get_mut().get_mut()) };
        R::poll(Context::new(root, wake, work, driver))
    }
}
