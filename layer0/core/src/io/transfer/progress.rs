use std::mem;

use o3::buffer::{self, segments::scatter};

use crate::io::transfer;

/// Contiguous transfer input whose progress is bound to this exact slice.
pub struct Bytes<'data> {
    bytes: &'data [u8],
    progress: State,
}

/// Prefix selected from one [`Bytes`] cursor and not yet committed.
#[must_use = "the selected bytes must either be committed or left untouched"]
pub struct Slice<'a, 'data> {
    bytes: &'data [u8],
    step: Step<'a>,
}

impl<'data> Bytes<'data> {
    #[must_use]
    pub fn new(view: transfer::View<'data>) -> Self {
        let (bytes, len) = view.into_parts();
        Self {
            bytes,
            progress: State::new(len),
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }

    pub fn take(&mut self, limit: transfer::Len) -> Slice<'_, 'data> {
        let start = self.bytes.len() - self.progress.left().into_raw();
        let step = self.progress.reserve(limit);
        let end = start + step.amount().into_raw();
        // SAFETY: `Bytes::new` binds the state limit to this exact slice.
        // `State::reserve` selects no more than its uncommitted remainder, so
        // `start..end` remains inside `bytes` and cannot overflow.
        let bytes = unsafe { self.bytes.get_unchecked(start..end) };
        Slice { bytes, step }
    }

    #[must_use]
    pub const fn into_len(self) -> transfer::Len {
        self.progress.len()
    }
}

impl Slice<'_, '_> {
    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        self.bytes
    }

    #[must_use]
    pub const fn amount(&self) -> transfer::Len {
        self.step.amount()
    }

    pub fn commit(self) {
        self.step.commit();
    }
}

const _: () =
    assert!(mem::size_of::<Bytes<'_>>() == mem::size_of::<&[u8]>() + mem::size_of::<State>());

/// Exact amount rejected by a bounded progress transition.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Exceeded {
    requested: transfer::Len,
    available: transfer::Len,
}

impl Exceeded {
    const fn new(req: transfer::Len, avail: transfer::Len) -> Self {
        Self {
            requested: req,
            available: avail,
        }
    }

    pub const fn requested(self) -> transfer::Len {
        self.requested
    }

    pub const fn available(self) -> transfer::Len {
        self.available
    }
}

/// Monotonic committed prefix of one bounded transfer.
pub struct State {
    limit: transfer::Len,
    remaining: transfer::Len,
}

impl State {
    #[must_use]
    pub const fn new(limit: transfer::Len) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.remaining.is_zero()
    }

    /// Returns the uncommitted suffix without discarding its backend bound.
    #[must_use]
    pub const fn left(&self) -> transfer::Len {
        self.remaining
    }

    #[must_use]
    pub const fn len(&self) -> transfer::Len {
        self.limit.abs_diff(self.remaining)
    }

    pub fn reserve(&mut self, limit: transfer::Len) -> Step<'_> {
        let amount = limit.min(self.remaining);
        Step {
            progress: self,
            amount,
        }
    }

    /// Advances an exact scatter cursor within this transfer's remaining budget.
    pub fn scatter<'a>(
        &mut self,
        cursor: &mut impl buffer::raw::Scatter<'a>,
    ) -> Option<buffer::raw::Chunk<'a, { transfer::MAX_BYTES }>> {
        let chunk = scatter::Take::new(cursor, self.remaining.into_raw())
            .next_chunk::<{ transfer::MAX_BYTES }>()?;
        let count = transfer::Len::from_count(chunk.len().into_count());
        self.remaining = self.remaining.abs_diff(count);
        Some(chunk)
    }

    /// Prepares an exact extended state when the whole amount fits.
    pub fn try_extend(&self, amount: transfer::Len) -> Result<Self, Exceeded> {
        if amount.is_greater(self.remaining) {
            return Err(Exceeded::new(amount, self.remaining));
        }
        Ok(Self {
            limit: self.limit,
            remaining: self.remaining.abs_diff(amount),
        })
    }
}

/// Prefix selected from one [`State`] and not yet committed.
pub struct Step<'a> {
    progress: &'a mut State,
    amount: transfer::Len,
}

impl Step<'_> {
    /// Returns the selected prefix without discarding its backend bound.
    #[must_use]
    pub const fn amount(&self) -> transfer::Len {
        self.amount
    }

    pub fn commit(self) {
        self.progress.remaining = self.progress.remaining.abs_diff(self.amount);
    }
}

const _: () = {
    assert!(mem::size_of::<State>() == 2 * mem::size_of::<u32>());
    let state = mem::size_of::<&mut State>() + mem::size_of::<u32>();
    let align = mem::align_of::<Step<'_>>();
    assert!(mem::size_of::<Step<'_>>() == state.div_ceil(align) * align);
};
