use std::{marker, pin};

use o3::{
    alloc,
    depot::{arena::slab::storage, capacity},
};

use crate::task::storage::{Id, caps};

pub mod direct;
pub mod local;
pub mod quota;
pub mod schedule;
pub mod staged;
mod store;
pub mod tree;

/// Exact admission state for one fixed task storage boundary.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Capacity {
    Available,
    Full,
}

/// Pinned task values with exact fixed capacity.
/// Slots, generations, and the free list belong to o3's arena extent.
#[pin_project::pin_project(!Unpin)]
pub struct Slab<'d, F, const N: usize, D, S: storage::Storage = storage::Inline<N>> {
    #[pin]
    inner: store::Store<F, N, D, S>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, const N: usize, D> Slab<'d, F, N, D> {
    /// Builds `N` pinned slots in place; this neither allocates nor fails.
    pub fn new() -> Self {
        Self {
            inner: store::Store::new(caps::Tasks::new()),
            driver: marker::PhantomData,
        }
    }
}

impl<'d, F, const N: usize, D> Default for Slab<'d, F, N, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'d, F, const N: usize, D> Slab<'d, F, N, D, storage::Heap> {
    /// Allocates the exact slot extent before constructing its entries.
    pub fn try_new() -> Result<Self, alloc::Error> {
        Self::try_new_with_capacity(capacity::Count::MAX)
    }

    fn try_new_with_capacity(physical: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            inner: store::Store::try_new(caps::Tasks::new(), physical)?,
            driver: marker::PhantomData,
        })
    }
}

/// One reserved task slot retaining the identity of the store that issued it.
struct Admit<'a, 'd, F, const N: usize, D, S: storage::Storage> {
    slot: store::Vacant<'a, F, N, D, S>,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d, F, const N: usize, D, S: storage::Storage> direct::Insert<'d, D>
    for Admit<'_, 'd, F, N, D, S>
{
    type Fiber = F;

    fn insert(self, fiber: F) -> Id<'d, D> {
        let (key, owner) = self.slot.insert(fiber);
        Id::from_raw(key, owner)
    }
}

impl<'d, F, const N: usize, D, S: storage::Storage> local::Core<'d, D> for Slab<'d, F, N, D, S> {
    type Fiber = F;

    fn capacity(&self) -> Capacity {
        self.inner.capacity()
    }

    fn insert(self: pin::Pin<&mut Self>, fiber: F) -> Option<Id<'d, D>> {
        let vacant = direct::Core::admit(self)?;
        Some(direct::Insert::insert(vacant, fiber))
    }

    fn try_remove(self: pin::Pin<&mut Self>, id: Id<'d, D>) -> Result<(), Id<'d, D>> {
        self.project().inner.try_remove(id)
    }

    fn get_mut(self: pin::Pin<&mut Self>, id: &Id<'d, D>) -> Option<pin::Pin<&mut F>> {
        Some(self.project().inner.member(id)?.into_value())
    }
}

impl<'d, F, const N: usize, D, S: storage::Storage> direct::Core<'d, D> for Slab<'d, F, N, D, S> {
    fn admit(self: pin::Pin<&mut Self>) -> Option<impl direct::Insert<'d, D, Fiber = F>> {
        let this = self.project();
        Some(Admit {
            slot: this.inner.vacant()?,
            driver: *this.driver,
        })
    }
}

impl<'d, F, const N: usize, D, S: storage::Storage> tree::Member<'d> for Slab<'d, F, N, D, S> {
    type Fiber = F;
    type Domain = D;
    type Target = ();

    const CAPACITY: tree::Capacity = tree::Capacity::new(caps::Tasks::<N>::new().nonzero());
}

impl<'d, F, const N: usize, D> tree::Build<'d> for tree::Leaf<Slab<'d, F, N, D, storage::Heap>> {
    fn try_build_with_capacity(physical: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self::new(Slab::try_new_with_capacity(physical)?))
    }
}

const _: () = assert!(std::mem::size_of::<Capacity>() == std::mem::size_of::<u8>());
