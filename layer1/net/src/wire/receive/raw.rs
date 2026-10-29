use std::num;

use o3::buffer::bytes;

use crate::wire::{
    self,
    receive::{self, batch, batch::Capacity},
};

/// Bounded receive batch with producer floor `MIN_CAPACITY`.
/// # Safety
/// `MIN_CAPACITY <= MAX_ITEMS <= 32` must hold for the declared item limits.
/// Exact `len <= MAX_ITEMS` changes only through `next`, consuming one item or returning `None` at zero.
pub unsafe trait Batch<'input> {
    type Item: bytes::TryRetain + 'input;

    /// Absolute maximum number of items returned by one receive call.
    const MAX_ITEMS: num::NonZero<usize> = num::NonZero::<usize>::MIN;

    /// Smallest per-call capacity under which the producer can consume valid input.
    const MIN_CAPACITY: num::NonZero<usize> = Self::MAX_ITEMS;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn next(&mut self) -> Option<Self::Item>;
}

/// # Safety
/// Own all counted output until returning; its length must fit this exact
/// capacity, using its value or the policy's statically guaranteed minimum.
pub unsafe trait Producer<'input, W: wire::Spec + 'input> {
    fn produce(
        self,
        capacity: &Capacity<'_, W>,
    ) -> <W::Receive as receive::Policy<W>>::Batch<'input>;
}

// SAFETY: the option discriminant is the exact remaining-item count;
// taking it consumes the sole item and leaves an empty batch.
unsafe impl<'input, T: bytes::TryRetain + 'input> receive::raw::Batch<'input> for batch::One<T> {
    type Item = T;

    fn len(&self) -> usize {
        usize::from(self.item.is_some())
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.item.take()
    }
}
