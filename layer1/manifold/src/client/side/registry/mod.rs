use o3::mem::quota;
mod store;
use std::{cell, task};

use dope_core::driver::{
    route::{
        Identity, KeyTag,
        table::{bounds::Excluded, conn::Capacity},
    },
    schedule::{app, maint, ready, ready::wake},
};
use dope_net::link::{egress::data, pool};
use o3::{alloc, depot::map::direct};
use store::Source;
pub(super) use store::Store;

use crate::client::{conn, side};

pub(in crate::client) enum Rejection {
    Empty,
    Settled,
}

#[repr(u8)]
pub(in crate::client) enum Queue {
    Empty,
    Pending,
}

pub(in crate::client) struct Pending {
    pub ready: bool,
    pub deferred: bool,
    pub cancel: bool,
}

impl Pending {
    pub(in crate::client) fn intake(&self) -> Queue {
        if self.ready || self.deferred {
            Queue::Pending
        } else {
            Queue::Empty
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Queue>() == std::mem::size_of::<bool>());
    assert!(std::mem::align_of::<Queue>() == std::mem::align_of::<bool>());
};

#[derive(Debug, Eq, PartialEq)]
pub enum Cause<'d, ID: Identity = KeyTag<0>> {
    Stale,
    Slot(direct::Error<pool::raw::Key<'d, ID>>),
}

/// Rejected reservation retaining its exact caller-owned values.
#[must_use]
pub struct Rejected<'d, I, ID: Identity = KeyTag<0>> {
    intent: I,
    waiter: wake::Waker<'d>,
    cause: Cause<'d, ID>,
}

impl<'d, I, ID: Identity> Rejected<'d, I, ID> {
    pub const fn cause(&self) -> &Cause<'d, ID> {
        &self.cause
    }

    pub fn into_parts(self) -> (I, wake::Waker<'d>) {
        (self.intent, self.waiter)
    }
}

/// Compact application intent materialized only when side transport admits it.
pub trait Intent<'d, ID: Identity = KeyTag<0>>: Sized {
    type Payload: data::raw::Payload<'d> + data::raw::StableOwner<'d> + 'd;

    fn target(&self) -> conn::Id<'d, ID>;

    fn into_payload(self) -> Self::Payload;
}

/// Fixed-capacity owner of one side request close per primary target.
pub struct Registry<'d, I: Intent<'d, ID>, ID: Identity = KeyTag<0>> {
    slots: Store<'d, I, I::Payload, ID>,
    ready: cell::Cell<Option<ready::Target<'d>>>,
}

impl<'d, I: Intent<'d, ID>, ID: Identity> Registry<'d, I, ID> {
    pub fn try_new(capacity: Capacity) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: Store::try_new(capacity.entries())?,
            ready: cell::Cell::new(None),
        })
    }

    pub fn ready_target(&self) -> Option<ready::Target<'d>> {
        self.ready.get()
    }

    pub fn try_reserve(
        &self,
        intent: I,
        waiter: wake::Waker<'d>,
    ) -> Result<(), Rejected<'d, I, ID>> {
        if self.ready.get().is_none() {
            return Err(Rejected {
                intent,
                waiter,
                cause: Cause::Stale,
            });
        }
        self.slots.try_reserve(intent, waiter)
    }

    pub fn poll(
        &self,
        target: conn::Id<'d, ID>,
        waiter: wake::Waker<'d>,
    ) -> task::Poll<Result<(), side::Error>> {
        self.slots.poll(target, waiter)
    }

    pub fn abandon(&self, target: conn::Id<'d, ID>) {
        self.slots.abandon(target);
    }

    pub(in crate::client) fn pending(&self) -> Pending {
        self.slots.pending()
    }

    pub(in crate::client) fn retry(&self) {
        self.slots.retry();
    }

    pub(in crate::client) fn start(&self, target: ready::Target<'d>) {
        self.ready.set(Some(target));
    }

    pub(in crate::client) fn take_request<'turn>(
        &self,
        work: app::Work<'turn, 'd>,
    ) -> quota::Admission<side::Request<'d, I::Payload, ID>> {
        work.run(|| {
            let (ticket, source) = self.slots.take_request()?;
            let payload = match source {
                Source::Intent(intent) => intent.into_payload(),
                Source::Payload(payload) => payload,
            };
            Some(side::Request { ticket, payload })
        })
    }

    pub(in crate::client) fn defer(&self, request: side::Request<'d, I::Payload, ID>) {
        self.slots.defer(request.ticket, request.payload);
    }

    pub(in crate::client) fn complete(
        &self,
        ticket: side::Ticket<'d, ID>,
        result: Result<(), side::Error>,
    ) {
        if let Some(waiter) = self.slots.complete(ticket, result) {
            waiter.wake();
        }
    }

    pub(in crate::client) fn take_cancel<'turn>(
        &self,
        work: maint::Work<'turn, 'd>,
    ) -> quota::Admission<side::Cancellation<'d, ID>> {
        work.run(|| self.slots.take_cancel())
    }

    pub(in crate::client) fn end_cancel(
        &self,
        cancel: side::Cancellation<'d, ID>,
        ticket: Option<side::Ticket<'d, ID>>,
    ) {
        let target = cancel.target();
        self.slots.end_cancel(cancel, Ok(()));
        if let Some(ticket) = ticket.filter(|ticket| ticket.target() != target) {
            self.complete(ticket, Err(side::Error::Stale));
        }
    }

    pub(in crate::client) fn fail_cancel(
        &self,
        cancel: side::Cancellation<'d, ID>,
        cause: Excluded,
    ) {
        self.slots
            .end_cancel(cancel, Err(side::Error::Route(cause)));
    }

    pub(in crate::client) fn close_intake(&self) {
        self.ready.set(None);
    }

    pub(in crate::client) fn reject(&self) -> Rejection {
        let Some(waiter) = self.slots.reject() else {
            return Rejection::Empty;
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Rejection::Settled
    }
}
