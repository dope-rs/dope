use std::cell;

use dope_core::io::transfer;

use crate::link::egress::{self, data};

/// The immutable owner and its exact unconsumed suffix from one occupied cell.
pub(in crate::link::egress) struct View<'a, T, P: data::position::State = ()> {
    pub(super) source: &'a egress::NonEmpty<T>,
    pub(super) remaining: transfer::NonZero,
    pub(super) position: &'a cell::Cell<P>,
}

impl<'a, T, P: data::position::State> View<'a, T, P> {
    pub(in crate::link::egress) fn chunks<'d>(self) -> T::Cursor<'a>
    where
        T: data::raw::Payload<'d, Position = P>,
    {
        let total = transfer::Len::from_nonzero(self.source.geometry().wire());
        let rest = transfer::Len::from_nonzero(self.remaining);
        let offset = data::Offset::from_len(total.abs_diff(rest));
        let mut position = self.position.get();
        // SAFETY: this cell resets its checkpoint whenever it replaces the owner.
        // Positive stored credit proves this exact immutable source has a suffix.
        let cursor = unsafe { self.source.as_inner().resume(&mut position, offset) };
        self.position.set(position);
        cursor
    }
}

const _: () = assert!(size_of::<View<'static, ()>>() == 3 * size_of::<usize>());
