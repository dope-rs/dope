pub mod raw;
mod sealed;

use std::{marker, num};

use crate::wire;

/// Per-call receive capacity branded by its wire type.
pub struct Capacity<W: wire::Wire> {
    items: num::NonZeroUsize,
    wire: marker::PhantomData<fn() -> W>,
}

impl<W: wire::Wire> Capacity<W> {
    /// Clamps available work to the wire's supported receive range.
    pub fn fit(available: usize) -> Option<Self> {
        let min = <W::RecvBatch<'static> as raw::Source>::MIN_CAPACITY;
        let max = <W::RecvBatch<'static> as raw::Source>::MAX_ITEMS;
        if min > max {
            return None;
        }
        let items = num::NonZeroUsize::new(available.min(max.get()))?;
        (items >= min).then_some(Self {
            items,
            wire: marker::PhantomData,
        })
    }

    /// Returns the wire's maximum supported receive capacity.
    pub fn full() -> Self {
        Self {
            items: <W::RecvBatch<'static> as raw::Source>::MAX_ITEMS,
            wire: marker::PhantomData,
        }
    }

    /// Returns the exact number of items admitted for this call.
    pub fn items(&self) -> num::NonZeroUsize {
        self.items
    }
}
