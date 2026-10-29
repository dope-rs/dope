use std::{array, num};

use dope_net::wire::receive;
use o3::buffer::bytes;

/// Exact two-item receive batch used by multi-output test policies.
pub struct Pair<T> {
    items: array::IntoIter<T, 2>,
}

impl<T> Pair<T> {
    pub(crate) fn new(items: [T; 2]) -> Self {
        Self {
            items: items.into_iter(),
        }
    }
}

// SAFETY: the array iterator reports exactly its remaining items, at most two.
unsafe impl<'input, T: bytes::TryRetain + 'input> receive::raw::Batch<'input> for Pair<T> {
    type Item = T;

    const MAX_ITEMS: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(1);

    fn len(&self) -> usize {
        self.items.len()
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next()
    }
}
