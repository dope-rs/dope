//! Linear reply value leases.

use std::mem::size_of;

use o3::mem;

use crate::reply::retain::pool;

/// A linear value owner whose payload and reply accounting cannot separate.
pub struct Lease<'arena, 'credits, 'region, T> {
    owned: (
        T,
        Option<mem::Lease<pool::LeaseEdge<'arena, 'credits, 'region>>>,
    ),
}

impl<'arena, 'credits, 'region, T> Lease<'arena, 'credits, 'region, T> {
    pub(in crate::reply) fn new(
        value: T,
        claim: Option<mem::Lease<pool::LeaseEdge<'arena, 'credits, 'region>>>,
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
        size_of::<Option<mem::Lease<pool::LeaseEdge<'static, 'static, 'static>>>>()
            == size_of::<mem::Lease<pool::LeaseEdge<'static, 'static, 'static>>>()
    );
    assert!(size_of::<Lease<'static, 'static, 'static, ()>>() == 4 * size_of::<usize>());
};
