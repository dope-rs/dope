use core::{marker, mem, num::NonZero};

use crate::core::driver::schedule::ready::task::topology;

#[repr(transparent)]
pub(in crate::task::storage::set) struct Index<D> {
    raw: NonZero<u32>,
    domain: marker::PhantomData<fn(D) -> D>,
}

impl<D> Clone for Index<D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D> Copy for Index<D> {}

impl<D> Eq for Index<D> {}

impl<D> PartialEq for Index<D> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<D> Index<D> {
    /// # Safety
    /// `raw` is not the compact `Index::END` sentinel.
    pub(in crate::task::storage::set) const unsafe fn from_offset(raw: u32) -> Self {
        Self {
            raw: unsafe { NonZero::new_unchecked(!raw) },
            domain: marker::PhantomData,
        }
    }

    pub(in crate::task::storage::set) const fn raw(self) -> u32 {
        !self.raw.get()
    }

    pub(in crate::task::storage::set) const fn position(self) -> NonZero<u32> {
        self.raw.cast_signed().wrapping_neg().cast_unsigned()
    }
}

unsafe impl<D> topology::raw::Index for Index<D> {
    fn raw(self) -> u32 {
        self.raw()
    }

    unsafe fn from_raw(raw: u32) -> Self {
        unsafe { Self::from_offset(raw) }
    }
}

const _: () = assert!(mem::size_of::<Index<()>>() == mem::size_of::<u32>());
const _: () = assert!(mem::size_of::<Option<Index<()>>>() == mem::size_of::<u32>());
