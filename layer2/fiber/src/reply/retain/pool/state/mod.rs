use std::{
    cell,
    mem::{MaybeUninit, size_of},
};

use o3::{mem, mem::rc};

use crate::reply::{
    arena::state::Index,
    retain::{self, grant::payload, pool, pool::returns::Life},
};

pub(super) mod raw;

pub(super) struct Entry<'credits> {
    index: Index,
    source: cell::Cell<MaybeUninit<&'credits retain::raw::State>>,
    pub(in crate::reply::retain::pool) life: Life,
    pub(in crate::reply::retain::pool) values: raw::Axis<payload::Values>,
    pub(in crate::reply::retain::pool) bytes: raw::Axis<payload::Bytes>,
    pub(in crate::reply::retain::pool) leases: Refs,
    pub(in crate::reply::retain::pool) released: pool::raw::Link<Index>,
}

impl<'credits> Entry<'credits> {
    pub(in crate::reply::retain::pool) fn new(index: Index) -> Self {
        Self {
            index,
            source: cell::Cell::new(MaybeUninit::uninit()),
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

    /// # Safety
    /// The admitted vacant slot has no remaining refs or unsettled release.
    pub(super) unsafe fn bind(&self, source: &'credits retain::raw::State) {
        self.source.set(MaybeUninit::new(source));
    }

    /// # Safety
    /// A bound Key or retained amount proves that this entry was initialized.
    pub(super) unsafe fn source(&self) -> &'credits retain::raw::State {
        unsafe { self.source.get().assume_init() }
    }

    pub(in crate::reply::retain::pool) fn is_active(&self) -> bool {
        self.values.is_active() || self.bytes.is_active() || self.leases.is_active()
    }
}

impl Drop for Entry<'_> {
    fn drop(&mut self) {
        use mem::raw::Amount as _;

        // Final Pool ownership excludes usable keys. A retained amount can
        // exist only after bind; an untouched vacant entry never reads source.
        if let Some(amount) = self.values.take() {
            unsafe { self.source().release(amount.get()) };
        }
        if let Some(amount) = self.bytes.take() {
            unsafe { self.source().release(amount.get()) };
        }
    }
}

const _: () = {
    assert!(size_of::<Entry<'_>>() == 8 * size_of::<usize>());
};

#[repr(transparent)]
pub(in crate::reply::retain::pool) struct Refs(mem::raw::Count);

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

    #[inline]
    pub(in crate::reply::retain::pool) fn try_hold(&self) -> Result<(), rc::Error> {
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

    pub(in crate::reply::retain::pool) fn release(&self) -> rc::Release {
        // SAFETY: only the matching Values, Bytes, or LeaseEdge destructor
        // reaches this private operation, transferring one original live edge.
        unsafe { mem::raw::Edge::release(self) }
    }
}

const _: () = assert!(size_of::<Refs>() == size_of::<u32>());
