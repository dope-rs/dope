use std::cell;

use o3::{alloc, alloc::BoxSliceExt as _, base::cell::region, buffer::resident};

use crate::driver::storage::Exhausted;

/// Driver-owned stable backing for resident accounting domains.
pub(crate) struct Arena {
    slots: Box<[cell::UnsafeCell<resident::Domain>]>,
    next: cell::Cell<usize>,
}

/// One-word authority for the resident domains of an exact driver generation.
#[derive(Clone, Copy)]
pub struct Alloc<'d> {
    arena: &'d Arena,
    proof: region::Proof<region::Identity<'d>>,
}

impl Arena {
    pub(in crate::driver) fn try_new(domains: usize) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: Box::try_default(domains)?,
            next: cell::Cell::new(0),
        })
    }

    /// Starts a driver generation after all preceding capabilities have
    /// expired and issues its exact region-branded allocator.
    pub(in crate::driver) fn enter<'d>(
        &'d mut self,
        token: &region::Token<region::Identity<'d>>,
    ) -> Alloc<'d> {
        self.next.set(0);
        Alloc {
            arena: self,
            proof: token.proof(),
        }
    }
}

impl<'d> Alloc<'d> {
    /// Claims one stable counter. A counter is issued at most once per region.
    pub fn try_budget(self, capacity: usize) -> Result<resident::Budget<'d>, Exhausted> {
        let index = self.arena.next.get();
        let Some(slot) = self.arena.slots.get(index) else {
            return Err(Exhausted::Domains {
                capacity: self.arena.slots.len(),
            });
        };
        self.arena.next.set(index + 1);
        // SAFETY: Arena resets the monotonic cursor only under its unique
        // generation borrow, and this branch advances it before returning, so
        // no other allocation in `'d` can select this slot.
        let domain: &'d mut resident::Domain = unsafe { &mut *slot.get() };
        domain
            .try_budget(capacity, self.proof)
            .map_err(|cause| Exhausted::Retained { index, cause })
    }
}

const _: () = assert!(size_of::<Alloc<'static>>() == size_of::<usize>());
