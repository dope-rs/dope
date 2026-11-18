//! Retained reply accounting and ownership.

use std::mem::ManuallyDrop;

use o3::depot::arena::linked::slots;

use crate::reply::{Item, retain::grant::payload, usage};

mod bytes;
pub(in crate::reply) mod grant;
mod lease;
pub(in crate::reply) mod pool;
pub(in crate::reply) mod raw;
mod retained;
mod values;

pub use bytes::Bytes;
pub use lease::{Lease, Node};
pub use retained::Retained;
pub use values::Values;

pub struct First<'s, 'arena, 'credits, 'region, D: usage::Footprint<Amount: usage::ValueAmount>> {
    pub(in crate::reply) value:
        ManuallyDrop<slots::Live<Item<D>, slots::Borrow<'arena, Item<D>, false>>>,
    pub(in crate::reply) root: crate::reply::raw::Root<'s, 'arena, 'credits, 'region>,
}

impl<D: usage::Footprint<Amount: usage::ValueAmount>> First<'_, '_, '_, '_, D> {
    pub fn get(&self) -> &D {
        &self.value.get().value
    }
}

/// Payload credits held separately from arriving rows until the owner is dropped.
/// The caller owns the matching byte storage and protocol-value records.
#[must_use = "dropping the reservation returns its payload credits"]
pub struct Reservation<'arena, 'credits, 'region> {
    lane: pool::Lane<'arena, 'credits, 'region>,
    source: &'credits raw::State,
    amount: payload::Failure,
}

impl Reservation<'_, '_, '_> {
    pub fn usage(&self) -> usage::Usage {
        self.amount.usage()
    }
}

impl Drop for Reservation<'_, '_, '_> {
    fn drop(&mut self) {
        // SAFETY: the private constructor admitted this unchanged amount from
        // this exact source; Reservation has no disarm or rebinding operation.
        unsafe { self.lane.release(self.source, self.amount) };
    }
}

/// The byte and protocol-value ownership transferred with one reply.
pub struct Held<'arena, 'credits, 'region> {
    pub(in crate::reply::retain) bytes: Option<Bytes<'arena, 'credits, 'region>>,
    pub(in crate::reply::retain) values: Option<Values<'arena, 'credits, 'region>>,
}

impl<'arena, 'credits, 'region> Held<'arena, 'credits, 'region> {
    const fn empty() -> Self {
        Self {
            bytes: None,
            values: None,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        Option<Bytes<'arena, 'credits, 'region>>,
        Option<Values<'arena, 'credits, 'region>>,
    ) {
        (self.bytes, self.values)
    }
}
