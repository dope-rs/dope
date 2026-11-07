//! Linear reply value leases.

use std::mem::size_of;

use o3::{depot::arena::linked::slots, mem};

use crate::reply::{
    Item,
    retain::{grant::payload, pool::LeaseEdge},
    usage,
};

/// A linear value owner whose payload and reply accounting cannot separate.
pub struct Lease<'arena, 'credits, 'region, T> {
    owned: (T, Option<mem::Lease<LeaseEdge<'arena, 'credits, 'region>>>),
}

/// A scalar reply retained in its original pool slot.
pub struct Node<'arena, 'credits, 'region, D: usage::Footprint<Amount = usage::Bytes>> {
    pub(in crate::reply) value: slots::Live<Item<D>, slots::Borrow<'arena, Item<D>, false>>,
    pub(in crate::reply) _claim:
        mem::Lease<LeaseEdge<'arena, 'credits, 'region, payload::Failure<usage::Bytes>>>,
}

impl<D: usage::Footprint<Amount = usage::Bytes>> Node<'_, '_, '_, D> {
    pub fn get(&self) -> &D {
        &self.value.get().value
    }
}

impl<'arena, 'credits, 'region, T> Lease<'arena, 'credits, 'region, T> {
    pub(in crate::reply) fn new(
        value: T,
        claim: Option<mem::Lease<LeaseEdge<'arena, 'credits, 'region>>>,
    ) -> Self {
        Self {
            owned: (value, claim),
        }
    }

    pub fn get(&self) -> &T {
        &self.owned.0
    }

    /// Crosses into caller-owned storage and releases driver accounting.
    pub fn into_inner(self) -> T {
        self.owned.0
    }
}

const _: () = {
    assert!(
        size_of::<Option<mem::Lease<LeaseEdge<'static, 'static, 'static>>>>()
            == size_of::<mem::Lease<LeaseEdge<'static, 'static, 'static>>>()
    );
    assert!(size_of::<Lease<'static, 'static, 'static, ()>>() == 4 * size_of::<usize>());
};
