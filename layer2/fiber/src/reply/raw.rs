use std::{mem::ManuallyDrop, ptr::NonNull};

use o3::{
    alloc,
    alloc::array,
    base::num::bounded,
    depot::{self, arena::linked, queue::pool::fifo},
};

use crate::reply::{
    Batch, Item, LaneIndex, Length, Slot,
    arena::retire,
    recv::outcome,
    retain::{First, pool},
    usage::{self, Footprint},
};

pub(in crate::reply) struct Complete<'arena, 'credits, 'region, D: Footprint> {
    data: NonNull<fifo::Fifo<'arena, Item<D>, false>>,
    payload: NonNull<pool::Payload<'arena, 'credits, 'region>>,
}

pub(in crate::reply) struct Root<'s, 'arena, 'credits, 'region> {
    payload: NonNull<pool::Payload<'arena, 'credits, 'region>>,
    retire: &'s retire::State,
}

pub(in crate::reply) struct Terminal<'s, 'arena, 'credits, 'region, D: Footprint> {
    data: NonNull<fifo::Fifo<'arena, Item<D>, false>>,
    root: Root<'s, 'arena, 'credits, 'region>,
}

impl<'arena, 'credits, 'region, D: Footprint> Complete<'arena, 'credits, 'region, D> {
    pub(in crate::reply) fn new<E>(
        completed: outcome::Completion<'_, '_, 'arena, 'credits, 'region, D, E>,
    ) -> Self {
        let Slot { state, owners, .. } = completed.slot;
        Self {
            data: NonNull::from(&mut state.data),
            payload: NonNull::from(&mut owners.payload),
        }
    }

    /// # Safety
    /// The original handle stays exclusively borrowed until its registration is
    /// cleared. Neither that registration nor its slot may be used again.
    pub(in crate::reply) unsafe fn bind<'s>(
        self,
        retire: &'s retire::State,
    ) -> Terminal<'s, 'arena, 'credits, 'region, D> {
        Terminal {
            data: self.data,
            root: Root {
                payload: self.payload,
                retire,
            },
        }
    }
}

impl<'s, 'arena, 'credits, 'region, D: Footprint> Terminal<'s, 'arena, 'credits, 'region, D> {
    pub(in crate::reply) fn collect(self) -> Batch<'s, 'arena, 'credits, 'region, D> {
        // SAFETY: the private transfer pairs these fields with their original root.
        // Complete removed the entry from order, and no retirement was queued.
        let contents = if unsafe { self.data.as_ref() }.is_empty() {
            drop(self);
            None
        } else {
            Some(self)
        };
        Batch { contents }
    }
}

impl<'s, 'arena, 'credits, 'region, D: Footprint<Amount = usage::Bytes>>
    Terminal<'s, 'arena, 'credits, 'region, D>
{
    pub(in crate::reply) fn first(self) -> Option<First<'s, 'arena, 'credits, 'region, D>> {
        let Self { mut data, mut root } = self;
        // SAFETY: the consumed root excludes every other same-entry mutation.
        let front = unsafe { data.as_mut() }.front()?;
        let amount = front.get().amount;
        let value = front.detach();
        // SAFETY: this exact detached item still carries its original full amount.
        unsafe { root.payload.as_mut().take_first(amount) };
        Some(First {
            value: ManuallyDrop::new(value),
            root,
        })
    }
}

impl Drop for Root<'_, '_, '_, '_> {
    fn drop(&mut self) {
        // SAFETY: only original-registration transfer constructs Root. The same
        // entry stays occupied through this unique retirement publication.
        unsafe { self.payload.as_ref() }.defer_retire(self.retire);
    }
}

impl<D: Footprint<Amount = usage::Bytes>> Drop for First<'_, '_, '_, '_, D> {
    fn drop(&mut self) {
        let (_, amount) = self.value.get().amount.split();
        // SAFETY: First owns the detached node once. Its original Root remains
        // live until after node destruction and the matching credit return.
        unsafe {
            ManuallyDrop::drop(&mut self.value);
            self.root.payload.as_ref().release_first(amount);
        }
    }
}

impl<D: Footprint> Terminal<'_, '_, '_, '_, D> {
    pub(in crate::reply) fn len(&self) -> Length {
        // SAFETY: the root excludes same-entry mutation while this owner is borrowed.
        unsafe { self.root.payload.as_ref() }.len()
    }

    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        // SAFETY: the original sum remains in the entry held by this root.
        unsafe { self.root.payload.as_ref() }.usage()
    }

    pub(in crate::reply) fn iter(&self) -> linked::Iter<'_, Item<D>> {
        // SAFETY: every returned borrow is bounded by this immutable root owner.
        unsafe { self.data.as_ref() }.iter()
    }

    fn map_next<R>(&mut self, make: impl FnOnce(&D) -> R) -> R {
        // SAFETY: consuming Batch excludes outward row borrows; the exact count
        // and FIFO stay paired through each callback and original-member removal.
        let Some(front) = (unsafe { self.data.as_mut() }).front() else {
            unreachable!("batch")
        };
        let result = make(&front.get().value);
        let amount = front.get().amount;
        drop(front.detach());
        // SAFETY: the node and payload are gone; their original amount remains
        // accounted by this sole root until this matching credit return.
        unsafe { self.root.payload.as_mut().remove(amount) }.retire();
        result
    }
}

impl<C: Footprint> Batch<'_, '_, '_, '_, C> {
    /// Maps this retained batch into one exactly sized allocation. Each slot's
    /// consuming insertion commits its value before further callback work.
    /// Visited rows retire after their callbacks; errors defer the remaining rows.
    pub fn try_map<T, E>(
        mut self,
        mut make: impl for<'slot> FnMut(&C, array::Slot<'slot, T>) -> Result<array::Written<'slot>, E>,
    ) -> Result<Result<Vec<T>, E>, alloc::Error> {
        let count = self.len().get();
        let block = array::Block::try_new(count, ())?;
        // SAFETY: this consumed batch retains the FIFO and its exact count together.
        // Each iteration borrows its current row through the callback, then retires
        // that row once. Callback failure leaves the remainder owned by Batch.
        let values = unsafe {
            alloc::raw::try_map(block, 0..count, move |slot, _| {
                let Some(owned) = self.contents.as_mut() else {
                    unreachable!("batch")
                };
                owned.map_next(|value| make(value, slot))
            })
        };
        Ok(values.map(array::Block::into_vec))
    }
}

// SAFETY: both conversions preserve the same bounded lane value unchanged.
unsafe impl depot::raw::Compact for LaneIndex {
    const MAX: u32 = u32::MAX - 2;

    fn into_raw(self) -> u32 {
        self.get() as u32
    }

    unsafe fn from_raw(raw: u32) -> Self {
        // SAFETY: Compact requires raw within this exact inclusive bound.
        Self::from(unsafe { bounded::raw::u32(raw) })
    }
}
