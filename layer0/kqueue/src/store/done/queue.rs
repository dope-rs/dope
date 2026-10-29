use std::marker;

use dope_sys::io::reactor::complete::{self, Dequeued, Event};
use o3::{alloc, depot::queue::ring};

use crate::store::done::{Entry, Vacancy};

/// Owns completion resources until draining binds them to a driver lifetime.
#[repr(transparent)]
pub struct Queue<E: Event, C: complete::raw::Capacity> {
    pending: ring::Fifo<Entry<E>>,
    capacity: marker::PhantomData<C>,
}

impl<E: Event, C: complete::raw::Capacity> complete::Queue for Queue<E, C> {
    type Event = E;
    type Capacity = C;
    type Vacancy<'a>
        = Vacancy<'a, E>
    where
        Self: 'a;

    fn try_new(capacity: C) -> Result<Self, alloc::Error> {
        Ok(Self {
            pending: ring::Fifo::try_new(capacity.count())?,
            capacity: marker::PhantomData,
        })
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn available(&self) -> usize {
        self.pending.capacity() - self.pending.len()
    }

    fn vacant(&mut self) -> Option<Self::Vacancy<'_>> {
        self.pending.vacant().map(Vacancy)
    }

    fn pop_front(&mut self) -> Option<Dequeued<E>> {
        self.pending.pop_front().map(|entry| {
            if entry.suppressed {
                Dequeued::Reclaim(entry.event)
            } else {
                Dequeued::Emit(entry.event)
            }
        })
    }

    fn suppress(&mut self, target: E::Key) {
        for entry in &mut self.pending {
            if entry.event.token() == Some(target) {
                entry.suppressed = true;
            }
        }
    }

    fn is_terminal(&self, target: E::Key) -> bool {
        (&self.pending)
            .into_iter()
            .any(|entry| entry.event.token() == Some(target) && entry.event.is_terminal())
    }
}
