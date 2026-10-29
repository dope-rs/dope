use o3::{alloc, alloc::array, base::num::bounded, depot};

use crate::reply::{
    Batch, Drain, LaneIndex, State,
    retain::{self, pool::Owner},
};

pub(in crate::reply) fn try_map<C, T, E>(
    batch: &Batch<'_, '_, '_, '_, C>,
    mut make: impl for<'slot> FnMut(&C, array::Slot<'slot, T>) -> Result<array::Written<'slot>, E>,
) -> Result<Result<Vec<T>, E>, alloc::Error> {
    let block = array::Block::try_new(batch.len().get(), ())?;
    // SAFETY: Batch detaches one completed slot's entire FIFO and payload sum
    // together. Each original item contributed exactly one count, and every
    // prior removal refunded that same item's amount. This shared Batch borrow
    // retains both owners and the immutable detached partition through mapping
    // and cleanup; neither its count nor its iterator can come from elsewhere.
    let values = unsafe {
        alloc::raw::AssumeInit::try_map(block, batch.iter(), move |slot, item| make(item, slot))
    };
    Ok(values.map(array::Block::into_vec))
}

// SAFETY: both conversions preserve the same bounded lane value unchanged.
unsafe impl depot::raw::Compact for LaneIndex {
    const MAX: u32 = u32::MAX - 2;

    fn into_raw(self) -> u32 {
        self.get() as u32
    }

    unsafe fn from_raw(raw: u32) -> Self {
        // SAFETY: Compact requires raw within this exact inclusive bound.
        Self::from(unsafe { bounded::raw::Proof::assume(raw) }.retain())
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
