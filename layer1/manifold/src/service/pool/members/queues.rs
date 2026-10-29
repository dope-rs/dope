use dope_core::driver::schedule::clock;
use o3::{
    depot,
    depot::{arena::slab::key, capacity, heap::keyed, queue::round::set},
};

use crate::service::{
    health,
    pool::members::{Member, Occupied, Unlinked, backoff, failure},
};

/// The heap owns backoff eligibility; health is ordering metadata captured at insertion.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Retry<'d> {
    pub(super) retry_at: clock::Deadline<'d>,
    health: health::Generation,
}

impl<'d> Retry<'d> {
    pub(super) const fn new(health: health::Generation, retry_at: clock::Deadline<'d>) -> Self {
        Self { retry_at, health }
    }
}

type Heap<'h, 'd> = keyed::Min<key::Handle<Member>, Retry<'d>, u32, capacity::Domain<'h>>;

pub(super) struct Queue<'h, 'd> {
    retries: Heap<'h, 'd>,
}

impl<'h, 'd> Queue<'h, 'd> {
    pub(super) const fn new(retries: Heap<'h, 'd>) -> Self {
        Self { retries }
    }

    pub(super) fn first(
        &mut self,
    ) -> Option<keyed::Occupied<'_, key::Handle<Member>, Retry<'d>, u32, capacity::Domain<'h>>>
    {
        self.retries.first_entry()
    }

    pub(super) fn backoff<I, A>(
        &mut self,
        failure: failure::Failure<'_, 'h, I, A>,
        retry: Retry<'d>,
    ) {
        let key = failure.key();
        // SAFETY: admitted Healthy/Probing residents have no retry in this joint owner.
        // That absent member also proves spare capacity in the fixed-domain heap.
        let (entry, failure) = keyed::Vacant::from_live(unsafe {
            depot::raw::Proof::new(((&mut self.retries, failure), key))
        });
        entry.insert(retry);
        failure.commit();
    }

    pub(super) fn recover<'a, I, A, const ROOT: bool>(
        &mut self,
        backoff: backoff::Backoff<'a, 'h, I, A, ROOT>,
    ) -> Unlinked<'a, 'h, I, A, ROOT> {
        let key = backoff.key();
        // SAFETY: admission retains the exact backed-off resident in this joint owner.
        // No retirement or reuse can run while its exclusive guard is retained.
        let (entry, backoff) = keyed::Occupied::from_live(unsafe {
            depot::raw::Proof::new(((&mut self.retries, backoff), key))
        });
        entry.remove();
        backoff.release()
    }

    pub(super) fn reorder<I, A, const ROOT: bool>(entry: Occupied<'_, 'h, I, A, ROOT>) {
        if let set::State::Ready(entry) = entry.state() {
            entry.requeue();
        }
    }

    pub(super) fn remove<'a, I, A, const ROOT: bool>(
        &mut self,
        entry: Occupied<'a, 'h, I, A, ROOT>,
    ) -> Unlinked<'a, 'h, I, A, ROOT> {
        match entry.state() {
            set::State::Ready(entry) => entry.remove(),
            set::State::Idle(entry) => match backoff::Backoff::try_new(entry) {
                Ok(backoff) => self.recover(backoff),
                Err(backoff::Unready::Probing(entry) | backoff::Unready::Exhausted(entry)) => entry,
            },
        }
    }

    /// A reserved replacement consumes the old retry before changing its generation.
    pub(super) fn retire(&mut self, key: capacity::Key<'h, key::Handle<Member>>) {
        self.retries.retire(key);
    }

    pub(super) fn retry_at(&self) -> Option<clock::Deadline<'d>> {
        self.retries.peek().map(|entry| entry.1.retry_at)
    }
}
