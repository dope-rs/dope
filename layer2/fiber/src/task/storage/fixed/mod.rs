use std::marker;

use o3::{
    alloc,
    base::marker::ThreadBound,
    depot::{arena::slab::storage, capacity},
};

use crate::task::storage::caps;

pub mod direct;
mod id;
pub mod local;
pub mod schedule;
mod store;

pub use id::Id;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Capacity {
    Available,
    Full,
}

/// Pinned task values with exact fixed capacity.
#[pin_project::pin_project(!Unpin)]
pub struct Slab<'d, F, const N: usize, D, S: storage::Storage = storage::Inline<N>> {
    #[pin]
    inner: store::Inner<F, D, S>,
    driver: marker::PhantomData<(fn(&'d ()) -> &'d (), ThreadBound)>,
}

impl<'d, F, const N: usize, D> Slab<'d, F, N, D> {
    pub fn new() -> Self {
        let _ = caps::Tasks::<N>::new();
        Self {
            inner: store::Inner::default(),
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
    pub fn try_new() -> Result<Self, alloc::Error> {
        Self::try_new_with_capacity(capacity::Count::MAX)
    }

    pub fn try_new_with_capacity(physical: capacity::Count) -> Result<Self, alloc::Error> {
        let count = physical.min(capacity::Count::new(
            caps::Tasks::<N>::new().nonzero().get(),
        ));
        Ok(Self {
            inner: store::Inner::try_new(count)?,
            driver: marker::PhantomData,
        })
    }
}

impl<F, const N: usize, D, S: storage::Storage> Slab<'_, F, N, D, S> {
    pub fn capacity(&self) -> Capacity {
        if self.inner.is_full() {
            Capacity::Full
        } else {
            Capacity::Available
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Capacity>() == std::mem::size_of::<u8>());
    assert!(
        std::mem::size_of::<Slab<'static, u8, 1, ()>>()
            == std::mem::size_of::<store::Inner<u8, (), storage::Inline<1>>>()
    );
};
