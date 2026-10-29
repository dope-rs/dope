use std::mem;

use dope_core::driver::route::{
    Identity, KeyTag,
    table::{self, conn},
};

use crate::client::{attempt, conn::Id};
pub(in crate::client) mod complete;
mod error;
pub mod mode;
pub(crate) mod startup;
pub use error::Error;
pub mod registry;
pub(in crate::client) mod turn;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Capacities {
    primary: conn::Capacity,
    total: conn::Capacity,
}

impl Capacities {
    pub(crate) const fn standard(primary: conn::Capacity) -> Self {
        Self {
            primary,
            total: primary,
        }
    }

    pub(crate) const fn primary(self) -> conn::Capacity {
        self.primary
    }

    pub(crate) const fn total(self) -> conn::Capacity {
        self.total
    }
}

/// Primary and cancellation-slot capacities proved valid as one side domain.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Capacity(Capacities);

impl Capacity {
    pub fn try_new(primary: conn::Capacity) -> Result<Self, table::Invalid> {
        let total = conn::Capacity::try_from(primary.scale::<2>())?;
        Ok(Self(Capacities { primary, total }))
    }

    pub const fn primary(self) -> conn::Capacity {
        self.0.primary()
    }

    pub(crate) const fn into_inner(self) -> Capacities {
        self.0
    }
}

#[must_use = "an side request must be submitted or settled terminally"]
pub struct Request<'d, B, ID: Identity = KeyTag<0>> {
    ticket: Ticket<'d, ID>,
    payload: B,
}

impl<'d, B, ID: Identity> Request<'d, B, ID> {
    pub const fn target(&self) -> Id<'d, ID> {
        self.ticket.target
    }

    pub(in crate::client::side) fn reject(self) -> Ticket<'d, ID> {
        self.ticket
    }
}

#[must_use = "an side ticket must be settled terminally"]
pub struct Ticket<'d, ID: Identity = KeyTag<0>> {
    target: Id<'d, ID>,
}

impl<'d, ID: Identity> Ticket<'d, ID> {
    pub const fn target(&self) -> Id<'d, ID> {
        self.target
    }
}

/// Affine authority to finish one abandoned side request.
#[must_use = "an side cancellation must be finished terminally"]
pub(in crate::client) struct Cancellation<'d, ID: Identity = KeyTag<0>> {
    target: Id<'d, ID>,
}

impl<'d, ID: Identity> Cancellation<'d, ID> {
    pub(in crate::client) const fn target(&self) -> Id<'d, ID> {
        self.target
    }
}

pub struct Disabled;

#[repr(transparent)]
pub struct Enabled<R> {
    pub(crate) registry: R,
}
impl<'r, 'd, I: registry::Intent<'d, ID>, ID: Identity> Enabled<&'r registry::Registry<'d, I, ID>> {
    pub const fn new(registry: &'r registry::Registry<'d, I, ID>) -> Self {
        Self { registry }
    }
}
#[repr(transparent)]
pub struct Primary<'d, ID: Identity>(attempt::Id<'d, ID>);

impl<'d, ID: Identity> Primary<'d, ID> {
    pub(in crate::client) const fn new(attempt: attempt::Id<'d, ID>) -> Self {
        Self(attempt)
    }

    pub(in crate::client) const fn attempt(&self) -> attempt::Id<'d, ID> {
        self.0
    }
}

const _: () = assert!(
    mem::size_of::<Cancellation<'static, KeyTag<0>>>() == mem::size_of::<Id<'static, KeyTag<0>>>()
);
const _: () = assert!(mem::size_of::<Disabled>() == 0);
const _: () =
    assert!(mem::size_of::<(Disabled, (), conn::Capacity)>() == mem::size_of::<conn::Capacity>());
const _: () =
    assert!(mem::align_of::<(Disabled, (), conn::Capacity)>() == mem::align_of::<conn::Capacity>());
const _: () = assert!(mem::size_of::<Capacity>() == 2 * mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<Enabled<u64>>() == mem::size_of::<u64>());
const _: () = assert!(mem::align_of::<Enabled<u64>>() == mem::align_of::<u64>());
const _: () = assert!(
    mem::size_of::<Primary<'static, KeyTag<0>>>()
        == mem::size_of::<attempt::Id<'static, KeyTag<0>>>()
);
