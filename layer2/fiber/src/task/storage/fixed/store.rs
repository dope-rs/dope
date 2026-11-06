use std::{marker, mem, pin};

use o3::{
    alloc,
    base::marker::ThreadBound,
    depot::{
        arena::slab::{self, key, storage},
        capacity,
    },
};

use crate::task::storage::{Id, Owner, caps, fixed};

/// One pinned task slot extent in o3's arena slab.
type Inner<T, Tag, S> = slab::Pinned<T, Tag, { u32::MAX }, false, 1, S>;

/// One reserved slot reported before its fiber is installed.
type Anchor<'a, T, Tag, S> = slab::Anchor<'a, T, Tag, { u32::MAX }, false, 1, S>;

pub(super) type Member<'a, T, Tag, S> = slab::Anchored<'a, T, Tag, { u32::MAX }, false, 1, S>;

/// Pinned task slots backed by one exact o3 arena extent.
///
/// Only the fiber-facing capacity proof and this store's own `Owner` address
/// stay here: slots, generations, and the free list belong to o3.
#[pin_project::pin_project(!Unpin)]
pub(super) struct Store<T, const N: usize, Tag, S: storage::Storage = storage::Inline<N>> {
    #[pin]
    inner: Inner<T, Tag, S>,
    local: marker::PhantomData<ThreadBound>,
}

impl<T, const N: usize, Tag> Store<T, N, Tag> {
    /// Builds `N` pinned slots in place; this neither allocates nor fails.
    pub(in crate::task::storage::fixed) fn new(_: caps::Tasks<N>) -> Self {
        Self {
            inner: Inner::default(),
            local: marker::PhantomData,
        }
    }
}

impl<T, const N: usize, Tag> Store<T, N, Tag, storage::Heap> {
    pub(in crate::task::storage::fixed) fn try_new(
        capacity: caps::Tasks<N>,
        physical: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        let count = physical.min(capacity::Count::new(capacity.nonzero().get()));
        Ok(Self {
            inner: Inner::try_new(count)?,
            local: marker::PhantomData,
        })
    }
}

impl<T, const N: usize, Tag, S: storage::Storage> Store<T, N, Tag, S> {
    pub(in crate::task::storage::fixed) fn capacity(&self) -> fixed::Capacity {
        if self.inner.is_full() {
            fixed::Capacity::Full
        } else {
            fixed::Capacity::Available
        }
    }

    pub(in crate::task::storage::fixed) fn member<'d>(
        self: pin::Pin<&mut Self>,
        id: &Id<'d, Tag>,
    ) -> Option<Member<'_, T, Tag, S>> {
        let key = id.key_for(Owner::of(self.as_ref().get_ref()))?;
        self.project().inner.anchor(key.parts())
    }

    pub(in crate::task::storage::fixed) fn vacant(
        self: pin::Pin<&mut Self>,
    ) -> Option<Vacant<'_, T, N, Tag, S>> {
        let owner = Owner::of(self.as_ref().get_ref());
        Some(Vacant {
            anchor: self.project().inner.vacant()?,
            owner,
        })
    }

    pub(in crate::task::storage::fixed) fn try_remove<'d>(
        self: pin::Pin<&mut Self>,
        id: Id<'d, Tag>,
    ) -> Result<(), Id<'d, Tag>> {
        let Some(anchor) = self.member(&id) else {
            return Err(id);
        };
        anchor.discard();
        Ok(())
    }
}

/// One reserved task slot retaining the identity of the store that issued it.
/// The wrapper keeps that owner address, which o3's keys do not carry.
pub(super) struct Vacant<'a, T, const N: usize, Tag, S: storage::Storage = storage::Inline<N>> {
    anchor: Anchor<'a, T, Tag, S>,
    owner: Owner<Tag>,
}

impl<T, const N: usize, Tag, S: storage::Storage> Vacant<'_, T, N, Tag, S> {
    /// Installs the first value and returns its identity with the issuing owner,
    /// which the caller must retain to recognise this store again.
    pub(in crate::task::storage::fixed) fn insert(
        self,
        value: T,
    ) -> (key::Handle<Tag>, Owner<Tag>) {
        (self.anchor.insert(value), self.owner)
    }
}

const _: () = assert!(
    mem::size_of::<Store<u8, 1, ()>>() == mem::size_of::<Inner<u8, (), storage::Inline<1>>>()
);
