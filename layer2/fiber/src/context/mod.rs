use core::{cell, mem, pin, task};

use o3::base::cell::region;

use crate::{
    abi,
    core::driver::{
        retained,
        schedule::{
            app,
            ready::{self, wake::Waker},
        },
    },
};
pub mod raw;
pub mod wake;

mod pollcall;

pub use pollcall::PollCall;

/// One admitted child descent, retaining the original pinned poll context.
/// Consuming it transfers the parent's credit to a task that owns completion.
pub struct Admission<'call, 'turn, 'd: 'turn> {
    context: pin::Pin<&'call mut Context<'turn, 'd>>,
}

impl<'call, 'turn, 'd: 'turn> Admission<'call, 'turn, 'd> {
    pub(crate) fn into_context(self) -> pin::Pin<&'call mut Context<'turn, 'd>> {
        self.context
    }

    /// Borrows the context for setup before the admitted child descent.
    pub fn context(&mut self) -> pin::Pin<&mut Context<'turn, 'd>> {
        self.context.as_mut()
    }

    /// Transfers the existing credit exactly once and returns the context for
    /// subsequent work. A completed task remains pending without repolling its
    /// former fiber; additional child polls through the context pay a credit.
    pub fn poll<F: abi::Fiber<'d>>(
        mut self,
        fiber: pin::Pin<&mut abi::Task<F>>,
    ) -> (
        task::Poll<F::Output>,
        pin::Pin<&'call mut Context<'turn, 'd>>,
    ) {
        let result = <abi::Task<F> as abi::Fiber<'d>>::poll(PollCall {
            fiber,
            context: self.context.as_mut(),
        });
        (result, self.context)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Requeue {
    Idle,
    Requested,
}

const _: () = {
    assert!(mem::size_of::<Requeue>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Requeue>() == mem::align_of::<bool>());
};

/// Result of reserving one additional application credit.
#[must_use = "a continuation either carries a permit or reports exhaustion"]
pub enum Continue<'poll, 'd> {
    Permit(app::Permit<'poll, 'd>),
    Exhausted,
}

const _: () = {
    assert!(mem::size_of::<Continue<'static, 'static>>() == mem::size_of::<u8>());
};

/// Result of polling one child with the current application budget.
#[must_use = "a poll either carries its result or reports exhaustion"]
pub enum PollResult<T> {
    Polled(task::Poll<T>),
    Exhausted,
}

const _: () = {
    assert!(mem::size_of::<PollResult<()>>() == mem::size_of::<Option<task::Poll<()>>>());
};

#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Context<'poll, 'd: 'poll> {
    wake: wake::Waker<'d>,
    root: wake::RootWaker<'d>,
    work: app::Work<'poll, 'd>,
    driver: retained::Context<'poll, 'poll, 'd>,
    requeue: cell::Cell<Requeue>,
}

impl<'poll, 'd: 'poll> Context<'poll, 'd> {
    fn new(
        wake: wake::Waker<'d>,
        root: wake::RootWaker<'d>,
        work: app::Work<'poll, 'd>,
        driver: retained::Context<'poll, 'poll, 'd>,
    ) -> Self {
        Self {
            wake,
            root,
            work,
            driver,
            requeue: cell::Cell::new(Requeue::Idle),
        }
    }

    pub(crate) fn with_waker<R>(
        wake: wake::Waker<'d>,
        root: wake::RootWaker<'d>,
        work: app::Work<'poll, 'd>,
        driver: retained::Context<'poll, 'poll, 'd>,
        run: impl for<'context> FnOnce(pin::Pin<&'context mut Self>) -> R,
    ) -> R {
        let mut context = pin::pin!(Self::new(wake, root, work, driver));
        run(context.as_mut())
    }

    /// Runs one lexical poll scope rooted at `target`.
    ///
    /// The higher-ranked callback keeps the stack-pinned context and every
    /// derived borrow inside this scope.
    ///
    /// ```compile_fail
    /// use std::pin::Pin;
    /// use dope::core::driver::{retained, schedule::{app, ready}};
    /// use dope_fiber::context::Context;
    ///
    /// fn escape<'poll, 'd: 'poll>(
    ///     target: ready::Target<'d>,
    ///     work: app::Work<'poll, 'd>,
    ///     driver: retained::Context<'poll, 'poll, 'd>,
    /// ) -> Pin<&'poll mut Context<'poll, 'd>> {
    ///     Context::with_target(target, work, driver, |context| context)
    /// }
    /// ```
    pub fn with_target<R>(
        target: ready::Target<'d>,
        work: app::Work<'poll, 'd>,
        driver: retained::Context<'poll, 'poll, 'd>,
        run: impl for<'context> FnOnce(pin::Pin<&'context mut Self>) -> R,
    ) -> R {
        let root = wake::RootWaker::from(target);
        Self::with_waker(root.into(), root, work, driver, run)
    }

    pub const fn waker(&self) -> wake::Waker<'d> {
        self.wake
    }

    /// Returns the exact driver-owned root carried through this poll tree.
    /// It can own a persistent scheduler whose members outlive child adapters.
    pub fn root_waker(self: pin::Pin<&Self>) -> wake::RootWaker<'d> {
        self.get_ref().root
    }

    /// Returns a persistent wake capability for this poll target.
    pub fn completion(self: pin::Pin<&Self>) -> Waker<'d> {
        self.get_ref().wake.completion()
    }

    pub(crate) fn work(self: pin::Pin<&Self>) -> app::Work<'poll, 'd> {
        self.get_ref().work
    }

    pub fn driver(self: pin::Pin<&mut Self>) -> retained::Context<'_, '_, 'd> {
        self.project().driver.reborrow()
    }

    /// Polls a live child with one credit, or returns `Pending` after completion.
    /// Exhaustion reactivates the current wake target and returns `Pending`.
    pub fn poll<'call, F>(
        self: pin::Pin<&'call mut Self>,
        fiber: pin::Pin<&'call mut abi::Task<F>>,
    ) -> task::Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        match self.try_poll(fiber) {
            PollResult::Polled(poll) => poll,
            PollResult::Exhausted => task::Poll::Pending,
        }
    }

    /// Polls a live child with one credit; `Exhausted` reactivates its wake target.
    /// A completed task returns `Polled(Pending)` without consuming credit.
    pub fn try_poll<'call, F>(
        self: pin::Pin<&'call mut Self>,
        fiber: pin::Pin<&'call mut abi::Task<F>>,
    ) -> PollResult<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        fiber.poll_with(|fiber| {
            // SAFETY: Task retains its live slot and vacates it on Ready.
            unsafe { self::raw::Live::new(fiber).try_poll(self) }
        })
    }

    /// Reserves one additional application credit for continued inline work.
    /// Exhaustion requeues the current fiber before returning `Exhausted`.
    pub fn try_continue(self: pin::Pin<&Self>) -> Continue<'poll, 'd> {
        match self.get_ref().work.permit() {
            Some(permit) => Continue::Permit(permit),
            None => {
                self.get_ref().requeue();
                Continue::Exhausted
            }
        }
    }

    pub(crate) fn with_child<'call, R>(
        self: pin::Pin<&'call mut Self>,
        wake: wake::Waker<'d>,
        run: impl for<'child> FnOnce(pin::Pin<&'child mut Context<'call, 'd>>) -> R,
    ) -> R {
        let this = self.project();
        Context::with_waker(wake, *this.root, *this.work, this.driver.reborrow(), run)
    }

    pub fn region_token(self: pin::Pin<&mut Self>) -> &mut region::Token<region::Identity<'d>> {
        self.project().driver.region_token()
    }

    pub fn wake(&self) {
        self.wake.wake();
    }

    /// Requests exact requeue after the current fiber borrow ends. A storage
    /// boundary may consume this bit for an inline requeue; otherwise pinned
    /// drop preserves ordinary deferred wake semantics.
    pub(crate) fn requeue(&self) {
        self.requeue.set(Requeue::Requested);
    }

    pub(crate) fn take_requeue(&self) -> Requeue {
        self.requeue.replace(Requeue::Idle)
    }
}

#[pin_project::pinned_drop]
impl PinnedDrop for Context<'_, '_> {
    fn drop(self: pin::Pin<&mut Self>) {
        let this = self.project();
        if this.requeue.get() == Requeue::Requested {
            (*this.wake).wake();
        }
    }
}
