use o3::depot::{arena::slab::key, capacity};

use crate::service::pool::members::{Idle, Member, Unlinked};

pub(super) enum Unready<'a, 'h, I, A> {
    Probing(Unlinked<'a, 'h, I, A>),
    Exhausted(Unlinked<'a, 'h, I, A>),
}

/// Owns the actual unscheduled resident admitted to its joint owner's retry heap.
pub(super) struct Backoff<'a, 'h, I, A> {
    entry: Unlinked<'a, 'h, I, A>,
}

impl<'a, 'h, I, A> Backoff<'a, 'h, I, A> {
    pub(super) fn try_new(entry: Unlinked<'a, 'h, I, A>) -> Result<Self, Unready<'a, 'h, I, A>> {
        match entry.get().health.idle {
            Idle::Backoff => Ok(Self { entry }),
            Idle::Probing => Err(Unready::Probing(entry)),
            Idle::Exhausted => Err(Unready::Exhausted(entry)),
        }
    }

    pub(super) fn key(&self) -> capacity::Key<'h, key::Handle<Member>> {
        self.entry.key()
    }

    pub(super) fn release(mut self) -> Unlinked<'a, 'h, I, A> {
        self.entry.get_mut().health.idle = Idle::Exhausted;
        self.entry
    }
}
