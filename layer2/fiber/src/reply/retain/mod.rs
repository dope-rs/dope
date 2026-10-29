//! Retained reply accounting and ownership.

use o3::mem;

use crate::reply::{retain::grant::payload, usage};

mod batch;
mod bytes;
pub(in crate::reply) mod grant;
mod lease;
pub(in crate::reply) mod pool;
pub(in crate::reply) mod raw;
mod retained;
mod values;

pub(crate) use batch::Batch;
pub use bytes::Bytes;
pub use lease::Lease;
pub use retained::Retained;
pub use values::Values;

/// Payload credits held separately from arriving rows until the owner is dropped.
/// The caller owns the matching byte storage and protocol-value records.
#[must_use = "dropping the reservation returns its payload credits"]
pub struct Reservation<'arena, 'credits, 'region> {
    held: mem::Lease<Return<'arena, 'credits, 'region>>,
}

struct Return<'arena, 'credits, 'region>(pool::Lane<'arena, 'credits, 'region>);

impl<'arena, 'credits, 'region> Reservation<'arena, 'credits, 'region> {
    pub(in crate::reply) fn try_new(
        lane: pool::Lane<'arena, 'credits, 'region>,
        usage: usage::Usage,
    ) -> Result<Self, usage::Capacity> {
        let grant = lane.try_reserve(usage)?;
        Ok(Self {
            held: mem::Lease::new(Return(lane), grant),
        })
    }

    pub fn usage(&self) -> usage::Usage {
        self.held.item().amount().usage()
    }
}

impl<'credits> mem::Return for Return<'_, 'credits, '_> {
    type Item = payload::Grant<'credits, payload::Failure>;

    fn return_item(self, grant: Self::Item) {
        self.0.release(grant);
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
