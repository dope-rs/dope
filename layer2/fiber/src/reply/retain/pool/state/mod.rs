use std::mem::size_of;

use o3::{mem, mem::rc};

use crate::reply::{
    arena::state::Index,
    retain::{grant::payload, pool, pool::returns::Life},
};

pub(super) mod raw;

pub(super) struct Entry<'credits> {
    index: Index,
    pub(in crate::reply::retain::pool) life: Life,
    pub(in crate::reply::retain::pool) values: raw::Axis<'credits, payload::Values>,
    pub(in crate::reply::retain::pool) bytes: raw::Axis<'credits, payload::Bytes>,
    leases: Refs,
    pub(in crate::reply::retain::pool) released: pool::raw::Link<Index>,
}

impl<'credits> Entry<'credits> {
    pub(in crate::reply::retain::pool) fn new(index: Index) -> Self {
        Self {
            index,
            life: Life::new(),
            values: raw::Axis::new(),
            bytes: raw::Axis::new(),
            leases: Refs::new(),
            released: pool::raw::Link::new(),
        }
    }

    pub(in crate::reply::retain::pool) const fn index(&self) -> Index {
        self.index
    }

    pub(in crate::reply::retain::pool) fn is_active(&self) -> bool {
        self.values.is_active() || self.bytes.is_active() || self.leases.is_active()
    }

    pub(in crate::reply::retain::pool) fn try_lease(&self) -> Result<(), rc::Error> {
        self.leases.try_hold()
    }

    pub(in crate::reply::retain::pool) fn drop_lease(&self) -> rc::Release {
        self.leases.release()
    }
}

const _: () = {
    assert!(size_of::<Entry<'_>>() == 9 * size_of::<usize>());
};

#[repr(transparent)]
struct Refs(mem::raw::Count);

// SAFETY: each embedded owner domain always projects its own unchanged count.
unsafe impl mem::raw::Edge for Refs {
    fn count(&self) -> &mem::raw::Count {
        &self.0
    }
}

impl Refs {
    const fn new() -> Self {
        Self(mem::raw::Count::empty())
    }

    fn is_active(&self) -> bool {
        !mem::raw::Edge::is_empty(self)
    }

    fn try_hold(&self) -> Result<(), rc::Error> {
        if self.is_active() {
            // SAFETY: this local nonempty domain retains an existing owner;
            // no user code or interleaving can release it during acquisition.
            unsafe { mem::raw::Edge::try_retain(self) }?;
        } else {
            // SAFETY: the owning entry controls this empty local domain until
            // the successful hold publishes its first retained edge.
            unsafe { mem::raw::Edge::activate(self) };
        }
        Ok(())
    }

    fn release(&self) -> rc::Release {
        // SAFETY: only the matching Values, Bytes, or LeaseEdge destructor
        // reaches this private operation, transferring one original live edge.
        unsafe { mem::raw::Edge::release(self) }
    }
}

const _: () = assert!(size_of::<Refs>() == size_of::<u32>());
