use o3::buffer::{self, pool, storage};

use crate::link::egress::data;

/// Proof that an egress payload stays immutable until release.
/// # Safety
/// Keep `as_ref` stable through `'d` and charge storage retained by egress.
#[doc(hidden)]
pub unsafe trait Sealed<'d> {
    fn retained_bytes(&self) -> usize;
}

// SAFETY: Shared retains immutable ref-counted storage.
unsafe impl<'d> Sealed<'d> for storage::Shared {
    fn retained_bytes(&self) -> usize {
        self.resident_bytes()
    }
}

// SAFETY: Frozen retains one immutable pool slot.
unsafe impl<'d> Sealed<'d> for buffer::Frozen {
    fn retained_bytes(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: A cursor is uniquely owned. Moving it into egress removes the only
// safe mutation path until egress returns or drops the owner.
unsafe impl<'d, C: pool::Capacity> Sealed<'d> for pool::Cursor<C> {
    fn retained_bytes(&self) -> usize {
        self.len() + self.spare_capacity()
    }
}

// SAFETY: A shared static slice stays immutable and extends no reclaimable
// storage lifetime.
unsafe impl<'d> Sealed<'d> for &'static [u8] {
    fn retained_bytes(&self) -> usize {
        0
    }
}

// SAFETY: A shared static array stays immutable and extends no reclaimable
// storage lifetime.
unsafe impl<'d, const N: usize> Sealed<'d> for &'static [u8; N] {
    fn retained_bytes(&self) -> usize {
        0
    }
}

// SAFETY: Inline owns fixed storage and exposes mutation only through unique
// access. Moving it into egress removes that mutation path until drop.
unsafe impl<'d, const N: usize> Sealed<'d> for data::Inline<N> {
    fn retained_bytes(&self) -> usize {
        N
    }
}

// SAFETY: Either never changes the retention or immutability guarantees of
// the active payload owner.
unsafe impl<'d, A: Sealed<'d>, B: Sealed<'d>> Sealed<'d> for data::Either<A, B> {
    fn retained_bytes(&self) -> usize {
        match self {
            data::Either::Left(value) => value.retained_bytes(),
            data::Either::Right(value) => value.retained_bytes(),
        }
    }
}

// SAFETY: Cursor exposes mutation only through unique access, which moving it
// into egress removes from callers.
unsafe impl<'d> Sealed<'d> for data::Cursor {
    fn retained_bytes(&self) -> usize {
        self.resident_bytes()
    }
}

// SAFETY: Every Buffer variant remains immutable for the branded domain.
unsafe impl<'d> Sealed<'d> for data::Buffer<'d> {
    fn retained_bytes(&self) -> usize {
        match self {
            data::Buffer::Borrowed(_) => 0,
            data::Buffer::Shared(bytes) => bytes.resident_bytes(),
            data::Buffer::Frozen(bytes) => bytes.capacity(),
        }
    }
}
