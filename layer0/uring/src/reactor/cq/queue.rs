use std::marker;

use dope_sys::io::reactor::complete::{self, Drain};
use o3::base::num::bounded;

use crate::reactor::cq;

pub struct Queue<'a, T>(&'a mut cq::raw::State, marker::PhantomData<fn(T) -> T>);

impl<'a, T> Queue<'a, T> {
    pub(crate) const fn new(state: &'a mut cq::raw::State) -> Self {
        Self(state, marker::PhantomData)
    }
}

impl<T> Drain for Queue<'_, T> {
    type Entry = cq::Entry<T>;

    /// Acquires CQEs published by the kernel without touching the local head.
    fn acquire(&mut self) {
        self.0.acquire();
    }

    /// Publishes every CQE consumed through this queue.
    fn publish(&mut self) {
        self.0.publish();
    }

    /// Publishes consumed CQEs and then acquires completions produced since the
    /// preceding acquisition.
    fn refresh(&mut self) {
        self.0.publish();
        self.0.acquire();
    }

    fn len(&self) -> bounded::U32<0, { u32::MAX }> {
        bounded::U32::clamp(self.0.available as usize)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn pop(&mut self) -> Option<Self::Entry> {
        self.0.pop()
    }
}

impl<T> complete::Inspect for Queue<'_, T> {
    type Head<'a>
        = cq::raw::Head<'a, T>
    where
        Self: 'a;

    fn front(&mut self) -> Option<Self::Head<'_>> {
        self.0.front()
    }

    fn available(&self) -> bounded::U32<0, { u32::MAX }> {
        bounded::U32::clamp(self.0.observed() as usize)
    }
}
