use std::{marker, mem, num};

use dope_core::driver::schedule::ready;
use o3::alloc;

use crate::link::egress::{
    create,
    load::credit::wait::{store::Record, tree},
    metadata,
};

#[repr(transparent)]
pub(super) struct Bound<'d, R> {
    pub(in crate::link::egress::load::credit::wait::store) len: num::NonZero<u32>,
    lifetime: marker::PhantomData<(ready::Retained<'d>, R)>,
}

impl<'d, R> Bound<'d, R> {
    pub(in crate::link::egress::load::credit::wait::store) fn try_new(
        lanes: metadata::LaneCount,
    ) -> Result<Self, create::WaitLayout> {
        let stride = Self::stride();
        let Some(bytes) = stride.checked_mul(lanes.count().get()) else {
            return Err(create::WaitLayout::Product { lanes, stride });
        };
        let len = lanes.position();
        let maximum = isize::MAX as usize;
        if bytes > maximum {
            return Err(create::WaitLayout::Limit {
                lanes,
                size: bytes,
                maximum,
            });
        }
        Ok(Self {
            len,
            lifetime: marker::PhantomData,
        })
    }

    pub(in crate::link::egress::load::credit::wait::store) const fn stride() -> usize {
        mem::size_of::<R>() + mem::size_of::<tree::Node>()
    }
}

// SAFETY: construction bounds the exact nonzero allocation size; the record
// and node alignment and disjointness are proved at the store owner.
unsafe impl<'p, 'd> alloc::raw::Shape for Bound<'d, Record<'p, 'd>> {
    type Root = Record<'p, 'd>;
    type Tail = tree::Node;
    type Extra = ();
    type Last = ();

    fn len(&self) -> usize {
        self.len.get() as usize
    }
}
