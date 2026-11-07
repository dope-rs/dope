use o3::{alloc, alloc::array, base::num::bounded, depot};

use crate::reply::{
    Batch, Drain, LaneIndex, Owned, State,
    retain::{self, pool::Owner},
};

impl<C> Batch<'_, '_, '_, '_, C> {
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
                let Some(owned) = self.owned.as_ref() else {
                    unreachable!("batch")
                };
                let value = make(&owned.data.front().value, slot);
                self.owned = self.owned.take().and_then(Owned::retire);
                value
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

/// Exclusive retirement of one slot and its original resource owner.
pub(in crate::reply) struct Retire<'a, 'arena, 'credits, 'region, D, E> {
    state: &'a mut State<'arena, D, E>,
    owner: &'a mut Owner<'arena, 'credits, 'region>,
}

impl<'a, 'arena, 'credits, 'region, D, E> Retire<'a, 'arena, 'credits, 'region, D, E> {
    /// # Safety
    /// Both arguments must be the slot and owner from the same original entry.
    pub(in crate::reply) unsafe fn new(
        state: &'a mut State<'arena, D, E>,
        owner: &'a mut Owner<'arena, 'credits, 'region>,
    ) -> Self {
        Self { state, owner }
    }
}

impl<D, E> Retire<'_, '_, '_, '_, D, E> {
    pub(in crate::reply) fn advance(&mut self) -> Drain {
        if let Some(item) = self.state.take_data() {
            // SAFETY: the removed member and payload come from this exact owned slot.
            unsafe { retain::raw::Remove::remove(&mut self.owner.payload, item.amount) }.release();
            return Drain::Dropped;
        }
        let Some(item) = self.state.take_failure() else {
            return Drain::Empty;
        };
        // SAFETY: the removed member and payload come from this exact owned slot.
        unsafe { retain::raw::Remove::remove(&mut self.owner.payload, item.amount) }.release();
        Drain::Dropped
    }
}
