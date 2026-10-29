use core::{marker, mem, pin, task};

use o3::base::marker::ThreadBound;

use crate::{
    abi::{self, future},
    context::{self, Context},
};

/// B credits traverse at most O(B * D) frames at nesting depth D.
/// Code between awaits remains cooperative.
pub(super) struct Frame<'call, 'turn, 'd: 'turn, I> {
    context: pin::Pin<&'call mut Context<'turn, 'd>>,
    first: bool,
    scope: marker::PhantomData<fn(&mut I) -> &mut I>,
}

const _: () = {
    assert!(mem::size_of::<Frame<'static, 'static, 'static, ()>>() == 2 * mem::size_of::<usize>());
};

impl<'call, 'turn, 'd: 'turn, I> Frame<'call, 'turn, 'd, I> {
    pub(super) fn new(context: pin::Pin<&'call mut Context<'turn, 'd>>) -> Self {
        Self {
            context,
            first: true,
            scope: marker::PhantomData,
        }
    }
}

impl<'call, 'turn, 'd: 'turn, I> Frame<'call, 'turn, 'd, I> {
    pub(super) fn poll<F>(&mut self, fiber: pin::Pin<&mut F>) -> task::Poll<F::Output>
    where
        F: abi::Fiber<'d>,
    {
        if mem::replace(&mut self.first, false) {
            // SAFETY: the adapter's PollCall admits this initial descent once.
            // The compiler await owner ends child polling authority on Ready.
            unsafe { context::raw::Live::new(fiber).delegate(self.context.as_mut()) }
        } else {
            // SAFETY: subsequent awaits retain compiler-owned live children,
            // but each begins another application transition and pays a credit.
            unsafe { context::raw::Live::new(fiber).poll(self.context.as_mut()) }
        }
    }
}

pub struct Brand<'d, I> {
    scope: marker::PhantomData<fn(&'d mut I) -> &'d mut I>,
    thread: marker::PhantomData<ThreadBound>,
}

pub struct Seal<'d, I> {
    scope: marker::PhantomData<fn(&'d mut I) -> &'d mut I>,
    thread: marker::PhantomData<ThreadBound>,
}

impl<'d, I> Brand<'d, I> {
    /// # Safety
    /// The pair must remain affine inside one thread-bound generated state machine.
    pub const unsafe fn scope() -> (Self, Seal<'d, I>) {
        (
            Self {
                scope: marker::PhantomData,
                thread: marker::PhantomData,
            },
            Seal {
                scope: marker::PhantomData,
                thread: marker::PhantomData,
            },
        )
    }

    pub fn awaitable<'a, F>(fiber: F, _: &'a mut Self) -> future::Awaitable<'a, 'd, I, F> {
        future::Awaitable {
            fiber,
            marker: marker::PhantomData,
        }
    }
}

impl<'d, I> Seal<'d, I> {
    /// # Safety
    /// `future` must be the paired generated state machine and never retain or expose its poll waker.
    /// It may await only its `Brand` values or immediately awaited lexical async scopes obeying this rule recursively.
    pub unsafe fn future<F>(self, future: F) -> future::FiberAdapter<'d, I, F> {
        future::FiberAdapter { future, seal: self }
    }
}
