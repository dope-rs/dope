use std::task::Waker;

use crate::backend;

pub(crate) struct Pending<R> {
    inbox: Vec<(u32, R)>,
    waiters: Vec<(u32, backend::park::WakeRef)>,
}

impl<R> Default for Pending<R> {
    fn default() -> Self {
        Self {
            inbox: Vec::new(),
            waiters: Vec::new(),
        }
    }
}

pub(crate) enum Resolve<R> {
    Ready(R),
    Pending,
}

impl<R> Pending<R> {
    pub(crate) fn settle(&mut self, tag: u32, value: R) {
        self.inbox.push((tag, value));
        if let Some(pos) = self.waiters.iter().position(|(t, _)| *t == tag) {
            let (_, w) = self.waiters.swap_remove(pos);
            w.wake();
        }
    }

    pub(crate) fn poll(&mut self, tag: u32, waker: &Waker) -> Resolve<R> {
        if let Some(pos) = self.inbox.iter().position(|(t, _)| *t == tag) {
            let (_, value) = self.inbox.swap_remove(pos);
            return Resolve::Ready(value);
        }
        let wref = backend::park::WakeRef::verified(waker);
        match self.waiters.iter_mut().find(|(t, _)| *t == tag) {
            Some(slot) => slot.1 = wref,
            None => self.waiters.push((tag, wref)),
        }
        Resolve::Pending
    }

    /// Drop the waiter for `tag` and return its settled-but-unclaimed result, if
    /// the op had already completed. The caller owns any resource the result
    /// carries (e.g. an opened fd) and is responsible for releasing it.
    pub(crate) fn take(&mut self, tag: u32) -> Option<R> {
        self.waiters.retain(|(t, _)| *t != tag);
        let pos = self.inbox.iter().position(|(t, _)| *t == tag)?;
        Some(self.inbox.swap_remove(pos).1)
    }

    /// Drop the waiter and any settled result for `tag`. Returns `true` if a
    /// settled result was discarded (the op had already completed), letting a
    /// caller free op-held resources instead of waiting for a completion that
    /// will never arrive.
    pub(crate) fn cancel(&mut self, tag: u32) -> bool {
        self.take(tag).is_some()
    }
}
