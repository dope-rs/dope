use std::{mem, num::NonZero};

use o3::buffer::{
    self, Residency as _, hold, pool, resident, resident::bound, segments, segments::scatter,
    storage,
};

use crate::link::egress::data::{
    self, Bound, compose,
    compose::{Either, Tagged},
    exec, pair,
};

/// # Safety
/// Shared access fixes bytes, length, and charge; `start()` spans `len()`.
/// Offset cursors yield the exact nonempty suffix, or `None` at the end.
pub unsafe trait Payload<'d> {
    type Mode: exec::Mode;

    type Cursor<'a>: buffer::raw::Scatter<'a>
    where
        Self: 'a;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn start(&self) -> Self::Cursor<'_>;
    fn charge(&self) -> hold::Charge;
    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>>;

    /// # Safety
    /// The position was only used with this unchanged payload, and offset is
    /// strictly below its length; each implementor rebuilds from that proof.
    unsafe fn resume(
        &self,
        position: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_>;
}

/// # Safety
/// Bytes, charge, and metrics remain fixed while retained.
/// Moving preserves every byte's address, beyond `buffer::raw::Segment`'s contract.
pub unsafe trait StableOwner<'d>: AsRef<[u8]> + buffer::Residency {}

// SAFETY: Shared retains immutable backing or an immutable borrowed slice;
// moving its handle changes neither byte addresses nor its resident charge.
unsafe impl<'d> StableOwner<'d> for resident::Shared<'d> {}

// SAFETY: the shared allocation fixes its byte addresses and backing charge
// until the last owner releases it; shared access cannot change its extent.
unsafe impl<'d> StableOwner<'d> for storage::Shared {}

// SAFETY: this cursor owns its pool allocation. Moving the handle preserves
// that allocation; shared access fixes its initialized suffix and charge.
unsafe impl<'d, L> StableOwner<'d> for pool::Cursor<L> {}

// SAFETY: the active variant retains one complete cursor unchanged. Each
// operation forwards the same exact remaining-byte proof to that cursor.
unsafe impl<'a, L, R> buffer::raw::Scatter<'a> for compose::Either<L, R>
where
    L: buffer::raw::Scatter<'a>,
    R: buffer::raw::Scatter<'a>,
{
    fn remaining(&self) -> usize {
        match self {
            Self::Left(cursor) => cursor.remaining(),
            Self::Right(cursor) => cursor.remaining(),
        }
    }

    unsafe fn advance<const MAX: usize>(
        &mut self,
        cap: NonZero<usize>,
        left: NonZero<usize>,
    ) -> buffer::raw::Chunk<'a, MAX> {
        match self {
            // SAFETY: either branch is the complete cursor, so `left` is its
            // exact remaining length.
            Self::Left(cursor) => unsafe { cursor.advance(cap, left) },
            // SAFETY: same proof as the left branch.
            Self::Right(cursor) => unsafe { cursor.advance(cap, left) },
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<compose::Either<&'static [u8], &'static [u8]>>()
            == 3 * mem::size_of::<usize>()
    );
};

/// # Safety
/// Shared access fixes the complete slice and its retained allocation charge.
/// Cursors retain that borrow; moving the owner may change its byte addresses.
pub unsafe trait Contiguous<'d>: AsRef<[u8]> + buffer::Residency {}

// SAFETY: shared owners keep their bytes and exact allocation charge immutable.
unsafe impl<'d> Contiguous<'d> for resident::Shared<'d> {}
unsafe impl<'d> Contiguous<'d> for storage::Shared {}
unsafe impl<'d, L> Contiguous<'d> for pool::Cursor<L> {}
// SAFETY: a shared borrow fixes the slice extent, and `Residency::resident`
// reports zero because a borrowed slice retains no backing allocation.
unsafe impl<'d, 'a: 'd> Contiguous<'d> for &'a [u8] {}

// SAFETY: the owner contract fixes one contiguous slice and charge for every
// borrow. Offset::tail rejects empty suffixes and preserves that exact borrow.
unsafe impl<'d, T: Contiguous<'d>> Payload<'d> for T {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// Borrowed and static owners have zero retained allocation charge.
    fn charge(&self) -> hold::Charge {
        hold::Charge::from_owner(self.as_ref().len(), self.resident())
    }

    fn start(&self) -> Self::Cursor<'_> {
        self.as_ref()
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        offset.tail(self.as_ref())
    }

    unsafe fn resume(
        &self,
        _: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: resume requires an offset within this unchanged payload;
        // Contiguous fixes the slice length for the complete shared borrow.
        unsafe { self.as_ref().get_unchecked(offset.get()..) }
    }
}

// SAFETY: The owned buffer or sealed segment owner fixes its bytes and accounting; cursors borrow that same owner.
unsafe impl<'d, O> Payload<'d> for segments::Chain<O>
where
    O: buffer::raw::Segment + data::raw::StableOwner<'d>,
{
    type Mode = exec::Fallible;
    type Cursor<'a>
        = <segments::Chain<O> as buffer::raw::Chunks>::Cursor<'a>
    where
        Self: 'a;

    fn len(&self) -> usize {
        segments::Sequence::len(self)
    }

    fn charge(&self) -> hold::Charge {
        hold::Charge::from(self)
    }

    fn start(&self) -> Self::Cursor<'_> {
        buffer::raw::Chunks::start(self)
    }

    unsafe fn resume(
        &self,
        position: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: the caller retains this unchanged chain and its position;
        // StableOwner preserves every segment address throughout traversal.
        unsafe { position.resume(self, offset.get()) }
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        if offset.is_at_least(self.len()) {
            return None;
        }
        buffer::raw::Chunks::cursor(self, offset.get())
    }
}

/// A payload whose logical length does not exceed its retained byte charge.
/// # Safety
/// The resident bound holds for every value and shared access.
pub unsafe trait Resident<'d>: Payload<'d> {}

// SAFETY: the initialized suffix cannot exceed its retained pool allocation;
// the cursor reports that allocation's full capacity as its resident charge.
unsafe impl<'d, L> Resident<'d> for pool::Cursor<L> {}

// SAFETY: both slices retain exact suffixes of the validated prefix/body owner.
unsafe impl<'a> buffer::raw::Scatter<'a> for pair::Cursor<'a> {
    fn remaining(&self) -> usize {
        self.prefix.len() + self.body.len()
    }

    unsafe fn advance<const MAX: usize>(
        &mut self,
        cap: NonZero<usize>,
        left: NonZero<usize>,
    ) -> buffer::raw::Chunk<'a, MAX> {
        if let Some(child) = NonZero::new(self.prefix.len()) {
            // SAFETY: child is the prefix's exact remaining length; the smaller
            // cap preserves the caller's MAX bound and the prefix borrow.
            unsafe { buffer::raw::Scatter::advance(&mut self.prefix, cap.min(child), child) }
        } else {
            // SAFETY: the empty prefix proves left is the body's exact length.
            unsafe { buffer::raw::Scatter::advance(&mut self.body, cap, left) }
        }
    }
}

// SAFETY: The owned buffer or sealed segment owner fixes its bytes and accounting; cursors borrow that same owner.
unsafe impl<'d, T: data::raw::Payload<'d>, G: bound::Bound> data::raw::Payload<'d> for Bound<T, G> {
    type Mode = exec::Guaranteed<G>;

    type Cursor<'a>
        = T::Cursor<'a>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.as_inner().len()
    }

    fn charge(&self) -> hold::Charge {
        self.as_inner().charge()
    }

    fn start(&self) -> Self::Cursor<'_> {
        self.as_inner().start()
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        self.as_inner().cursor(offset)
    }

    unsafe fn resume(
        &self,
        position: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: the bound retains the same immutable payload throughout traversal.
        unsafe { self.as_inner().resume(position, offset) }
    }
}

// SAFETY: The retained variant or tag preserves the original payload and its exact cursor contract.
unsafe impl<'d, A: data::raw::Payload<'d>, B: data::raw::Payload<'d>> data::raw::Payload<'d>
    for Either<A, B>
{
    type Mode = exec::Fallible;
    type Cursor<'a>
        = Either<A::Cursor<'a>, B::Cursor<'a>>
    where
        Self: 'a;

    fn len(&self) -> usize {
        match self {
            Self::Left(value) => value.len(),
            Self::Right(value) => value.len(),
        }
    }

    fn charge(&self) -> hold::Charge {
        match self {
            Self::Left(value) => value.charge(),
            Self::Right(value) => value.charge(),
        }
    }

    fn start(&self) -> Self::Cursor<'_> {
        match self {
            Self::Left(value) => Either::Left(value.start()),
            Self::Right(value) => Either::Right(value.start()),
        }
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        match self {
            Self::Left(value) => value.cursor(offset).map(Either::Left),
            Self::Right(value) => value.cursor(offset).map(Either::Right),
        }
    }

    unsafe fn resume(
        &self,
        position: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: shared access preserves this exact variant and its owner.
        match self {
            Self::Left(value) => Either::Left(unsafe { value.resume(position, offset) }),
            Self::Right(value) => Either::Right(unsafe { value.resume(position, offset) }),
        }
    }
}

// SAFETY: The retained variant or tag preserves the original payload and its exact cursor contract.
unsafe impl<'d, T: data::raw::Payload<'d>, K> data::raw::Payload<'d> for Tagged<T, K> {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = T::Cursor<'a>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.payload.len()
    }

    fn charge(&self) -> hold::Charge {
        self.payload.charge()
    }

    fn start(&self) -> Self::Cursor<'_> {
        self.payload.start()
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        self.payload.cursor(offset)
    }

    unsafe fn resume(
        &self,
        position: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: this tag retains the same immutable payload throughout traversal.
        unsafe { self.payload.resume(position, offset) }
    }
}

// SAFETY: tagging delegates length and resident charge to the unchanged owner;
// T already proves that its logical length is bounded by that charge.
unsafe impl<'d, T: data::raw::Resident<'d>, K> data::raw::Resident<'d> for Tagged<T, K> {}

// SAFETY: the pooled prefix and sealed body fix bytes and charge; cursors
// traverse their exact concatenation. Construction checks both byte sums,
// and the prefix retains one nonzero-capacity pool slot even when empty.
unsafe impl<'d, C: buffer::raw::Segment> data::raw::Payload<'d> for pair::Source<C> {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = pair::Cursor<'a>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.prefix.len() + self.body.as_ref().len()
    }

    fn charge(&self) -> hold::Charge {
        let body = hold::Charge::from_split(self.body.resident());
        let logical = if body.owners == 0 {
            0
        } else {
            self.body.as_ref().len()
        };
        hold::Charge::from_commit(
            self.prefix.len() + logical,
            self.prefix.resident() + body.resident_bytes,
            1 + body.owners,
        )
    }

    fn start(&self) -> Self::Cursor<'_> {
        pair::Cursor {
            prefix: self.prefix.as_ref(),
            body: self.body.as_ref(),
        }
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        Some(match offset.split_at(self.prefix.len()) {
            data::Split::Before(offset) => pair::Cursor {
                prefix: offset.tail(self.prefix.as_ref())?,
                body: self.body.as_ref(),
            },
            data::Split::After(offset) => pair::Cursor {
                prefix: &[],
                body: offset.tail(self.body.as_ref())?,
            },
        })
    }

    unsafe fn resume(
        &self,
        _: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: resume places this offset strictly below the validated total
        // length, so the selected prefix or body suffix is in range and nonempty.
        match offset.split_at(self.prefix.len()) {
            data::Split::Before(offset) => pair::Cursor {
                prefix: unsafe { self.prefix.as_ref().get_unchecked(offset.get()..) },
                body: self.body.as_ref(),
            },
            data::Split::After(offset) => pair::Cursor {
                prefix: &[],
                body: unsafe { self.body.as_ref().get_unchecked(offset.get()..) },
            },
        }
    }
}
