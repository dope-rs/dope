use std::{marker, ops};

use dope::net::wire;
use o3::buffer::resident;

/// A retained cursor that exclusively borrows its `Io` until it is dropped.
#[must_use = "dropping a read lease discards unread bytes and returns receive credit"]
pub struct Lease<'io, 'd, W: wire::Wire + 'd> {
    cursor: W::RetainedRecv<'d>,
    _io: marker::PhantomData<&'io mut ()>,
}

impl<'io, 'd, W: wire::Wire + 'd> Lease<'io, 'd, W> {
    pub(crate) fn new(cursor: W::RetainedRecv<'d>) -> Self {
        Self {
            cursor,
            _io: marker::PhantomData,
        }
    }
}

impl<'d, W: wire::Wire + 'd> ops::Deref for Lease<'_, 'd, W> {
    type Target = W::RetainedRecv<'d>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl<'d, W: wire::Wire + 'd> ops::DerefMut for Lease<'_, 'd, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}

impl<'io, 'd, W: wire::Wire + 'd> wire::Cursor<'d> for Lease<'io, 'd, W> {
    fn chunk(&self) -> &[u8] {
        let chunk = wire::Cursor::chunk(&self.cursor);
        debug_assert!(
            wire::Cursor::remaining(&self.cursor) == 0 || !chunk.is_empty(),
            "a non-empty receive cursor must expose progress"
        );
        chunk
    }

    fn consume(&mut self, requested: usize) -> usize {
        let available = wire::Cursor::chunk(&self.cursor).len();
        let consumed = wire::Cursor::consume(&mut self.cursor, requested);
        debug_assert!(consumed <= requested.min(available));
        consumed
    }

    fn remaining(&self) -> usize {
        let remaining = wire::Cursor::remaining(&self.cursor);
        debug_assert!(wire::Cursor::chunk(&self.cursor).len() <= remaining);
        remaining
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        if range.start > range.end || range.end > wire::Cursor::remaining(&self.cursor) {
            return Err(wire::RetainError::Range);
        }
        wire::Cursor::retain(&self.cursor, range, budget)
    }
}
