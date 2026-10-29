use std::{marker, num};

use crate::driver::retained::flight::storage::is_layout_valid;

/// Packed index and generation bits echoed by an external completion source.
#[repr(transparent)]
#[derive(Debug)]
pub struct Echo<T: Copy, const INDEX_BITS: u32 = 32, const GENERATION_BITS: u32 = 32> {
    raw: num::NonZero<u64>,
    value: marker::PhantomData<fn() -> T>,
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32>
    Echo<T, INDEX_BITS, GENERATION_BITS>
{
    pub const fn expose(self) -> u64 {
        self.raw.get()
    }

    pub fn from_exposed(raw: u64) -> Option<Self> {
        if !is_layout_valid::<INDEX_BITS, GENERATION_BITS>() {
            return None;
        }
        let width = INDEX_BITS + GENERATION_BITS;
        if (width < u64::BITS && raw >= 1u64 << width) || raw >> INDEX_BITS == 0 {
            return None;
        }
        Some(Self {
            raw: num::NonZero::<u64>::new(raw)?,
            value: marker::PhantomData,
        })
    }

    pub(super) fn from_parts(index: u32, epoch: num::NonZero<u32>) -> Self {
        let raw = (u64::from(epoch.get()) << INDEX_BITS) | u64::from(index);
        // SAFETY: live entries always have a nonzero generation.
        let raw = unsafe { num::NonZero::<u64>::new_unchecked(raw) };
        Self {
            raw,
            value: marker::PhantomData,
        }
    }

    pub(super) const fn index(self) -> u32 {
        let mask = if INDEX_BITS == u32::BITS {
            u32::MAX as u64
        } else {
            (1u64 << INDEX_BITS) - 1
        };
        (self.raw.get() & mask) as u32
    }

    pub(super) const fn generation(self) -> num::NonZero<u32> {
        // SAFETY: both constructors prove a nonzero generation within its bit width.
        unsafe { num::NonZero::<u32>::new_unchecked((self.raw.get() >> INDEX_BITS) as u32) }
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Clone
    for Echo<T, INDEX_BITS, GENERATION_BITS>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Copy
    for Echo<T, INDEX_BITS, GENERATION_BITS>
{
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> PartialEq
    for Echo<T, INDEX_BITS, GENERATION_BITS>
{
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<T: Copy, const INDEX_BITS: u32, const GENERATION_BITS: u32> Eq
    for Echo<T, INDEX_BITS, GENERATION_BITS>
{
}
