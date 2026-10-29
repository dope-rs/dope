use std::{marker, mem};

use dope::{
    core::driver::route::{Identity, KeyTag},
    manifold::client::conn,
};
use o3::alloc::array;

use crate::reply::{self, LaneIndex, layout};

/// A connection whose physical index belongs to its original reply arena.
///
/// ```compile_fail
/// use dope::manifold::client::conn;
/// use dope::core::driver::route::KeyTag;
/// use dope_fiber::reply::layout;
/// fn escape<'arena, 'region>(domain: layout::Domain<'arena>, id: conn::Id<'region, KeyTag<0>>)
///     -> Result<layout::Position<'static, 'region, KeyTag<0>>, dope_fiber::reply::Error> {
///     layout::Position::try_new(domain, id)
/// }
/// ```
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Position<'arena, 'region, ID: Identity> {
    connection: conn::Id<'region, ID>,
    owner: marker::PhantomData<fn(&'arena ()) -> &'arena ()>,
}

impl<'arena, 'region, ID: Identity> Position<'arena, 'region, ID> {
    /// Checks the physical index once and retains the exact arena domain.
    pub fn try_new(
        domain: layout::Domain<'arena>,
        id: conn::Id<'region, ID>,
    ) -> Result<Self, reply::Error> {
        LaneIndex::try_within(LaneIndex::from_conn(id.lane()), domain.capacity())?;
        Ok(Self {
            connection: id,
            owner: marker::PhantomData,
        })
    }

    pub const fn connection(self) -> conn::Id<'region, ID> {
        self.connection
    }

    /// Projects the checked position into storage of its original arena domain.
    pub fn entry<'a, T>(self, entries: &'a array::Block<T, layout::Domain<'arena>>) -> &'a T {
        // SAFETY: try_new checked this connection's index against the immutable
        // extent retained by both invariant arena brands.
        unsafe { entries.get_unchecked(self.connection.index().into_usize()) }
    }

    /// Exclusively projects the checked position into its original arena domain.
    pub fn entry_mut<'a, T>(
        self,
        entries: &'a mut array::Block<T, layout::Domain<'arena>>,
    ) -> &'a mut T {
        layout::raw::entry_mut(self, entries)
    }
}

const _: () = assert!(
    mem::size_of::<Position<'static, 'static, KeyTag<0>>>()
        == mem::size_of::<conn::Id<'static, KeyTag<0>>>()
);
