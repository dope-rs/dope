use std::{mem, ptr};

use o3::collections::completion::narrow;

use crate::driver::route;

// SAFETY: Reference<'d> is issued by the pinned Driver that owns `arena`.
// Driver teardown drains every external completion before ending that scope,
// and all access remains confined to its runtime thread.
unsafe impl<'d>
    narrow::raw::ArenaOwner<'d, route::Token, { super::INDEX_BITS }, { super::GENERATION_BITS }>
    for super::Owner<'_, 'd>
{
    fn arena(self) -> ptr::NonNull<super::InnerArena> {
        let super::Owner { arena, driver } = self;
        let _ = driver;
        ptr::NonNull::from(arena)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Echo(super::InnerEcho);

const _: () = {
    assert!(mem::size_of::<Echo>() == mem::size_of::<usize>());
};

impl Echo {
    pub(super) const fn from_inner(echo: super::InnerEcho) -> Self {
        Self(echo)
    }

    pub(super) const fn into_inner(self) -> super::InnerEcho {
        self.0
    }

    pub(crate) const fn raw(self) -> u64 {
        self.0.expose()
    }

    pub(crate) unsafe fn from_kernel(raw: u64) -> Option<Self> {
        Some(Self(super::InnerEcho::from_exposed(raw)?))
    }
}
