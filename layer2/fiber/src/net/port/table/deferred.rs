use crate::{
    manifold::app::dispatch::identity,
    net::port::{table, table::queues},
};

pub(super) enum Deferred<I: identity::Identity> {
    Direct,
    Queue(queues::Fifo<I, queues::Deferred>),
}

impl<I: identity::Identity> Deferred<I> {
    pub(super) const fn new(queue: bool) -> Self {
        if queue {
            Self::Queue(queues::Fifo::new())
        } else {
            Self::Direct
        }
    }

    pub(super) fn notify<'a, 'd, E>(
        &self,
        entries: &'a table::raw::Entries<'d, E, I>,
        bound: &queues::Bound<'a, 'd, E, I>,
    ) {
        let Self::Queue(queue) = self else {
            if let Some(wake) = bound.root_wake.get() {
                wake.wake();
            }
            return;
        };
        queue.push(entries, bound, |entry| &entry.deferred);
    }

    pub(super) fn pop<'a, 'd, E>(
        &'a self,
        entries: &'a table::raw::Entries<'d, E, I>,
    ) -> Option<queues::Bound<'a, 'd, E, I>> {
        let Self::Queue(queue) = self else {
            return None;
        };
        Some(queue.front(entries, |entry| &entry.deferred)?.pop())
    }

    pub(super) fn is_ready(&self) -> bool {
        matches!(self, Self::Queue(queue) if queue.is_ready())
    }
}
