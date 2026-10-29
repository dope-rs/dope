//! Trusted connection-lane projection from a shared layout proof.

use std::mem;

use dope::{core::driver::route::Identity, manifold::client::conn};
use o3::alloc::{self, array};

use crate::reply::{
    Arena, Lane, LaneIndex,
    layout::{Domain, Position},
};

pub(super) fn entry_mut<'a, 'arena, ID: Identity, T>(
    position: Position<'arena, '_, ID>,
    entries: &'a mut array::Block<T, Domain<'arena>>,
) -> &'a mut T {
    // SAFETY: the checked physical position and immutable allocation extent
    // retain the same invariant arena brand; the owner is exclusively borrowed.
    unsafe {
        &mut *entries
            .as_mut_ptr()
            .add(position.connection().index().into_usize())
    }
}

/// Borrowed raw projection of one reply arena.
#[repr(transparent)]
pub struct View<'a, 'arena, 'credits, 'region, D, E = D, K = ()> {
    arena: &'a Arena<'arena, 'credits, 'region, D, E, K>,
}

impl<'a, 'arena, 'credits, 'region, D, E, K> View<'a, 'arena, 'credits, 'region, D, E, K> {
    pub(in crate::reply) const fn new(
        arena: &'a Arena<'arena, 'credits, 'region, D, E, K>,
    ) -> Self {
        Self { arena }
    }

    /// Projects a connection lane whose owner proved its full storage domain.
    ///
    /// # Safety
    /// `lane` must belong to this arena's configured lane domain.
    pub unsafe fn lane<ID: Identity>(
        self,
        lane: conn::Lane<'region, ID>,
    ) -> Lane<'a, 'arena, 'credits, 'region, D, E, K> {
        Lane {
            arena: self.arena,
            index: LaneIndex::from_conn(lane),
        }
    }
}

// SAFETY: the generative arena's immutable lane count is retained in every copy.
unsafe impl alloc::raw::Extent for Domain<'_> {
    fn len(&self) -> usize {
        self.capacity().get() as usize
    }
}

const _: () = assert!(
    mem::size_of::<View<'static, 'static, 'static, 'static, ()>>() == mem::size_of::<usize>()
);
