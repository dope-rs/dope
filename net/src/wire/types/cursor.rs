use std::ops;

use dope_core::io::recv;
use o3::buffer::{PrefixConsumer as _, bytes, resident, storage};

use crate::wire;

/// Cursor over bytes owned by one resident snapshot.
/// Its exclusive owner borrow makes view/owner drift unrepresentable.
///
/// ```compile_fail
/// use dope_net::wire::ResidentCursor;
/// use o3::buffer::resident::Snapshot;
///
/// fn escape<'d, const N: usize>(owner: &mut Snapshot<'d, N>)
///     -> ResidentCursor<'static, 'd, N>
/// {
///     ResidentCursor::new(owner)
/// }
/// ```
#[must_use = "dropping a partially consumed cursor leaves its remaining owner bytes intact"]
pub struct ResidentCursor<'owner, 'd, const MAX_CAPACITY: usize> {
    cursor: storage::Shared,
    owner: &'owner mut resident::Snapshot<'d, MAX_CAPACITY>,
}

impl<'owner, 'd, const MAX_CAPACITY: usize> ResidentCursor<'owner, 'd, MAX_CAPACITY> {
    pub fn new(owner: &'owner mut resident::Snapshot<'d, MAX_CAPACITY>) -> Self {
        let cursor = owner.snapshot().unwrap_or_default();
        Self { cursor, owner }
    }
}

impl<'d, const MAX_CAPACITY: usize> wire::Cursor<'d> for ResidentCursor<'_, 'd, MAX_CAPACITY> {
    fn chunk(&self) -> &[u8] {
        self.cursor.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let requested = requested.min(self.cursor.len());
        let consumed = self.owner.consume_prefix_up_to(requested);
        let mirrored = self.cursor.consume_prefix_up_to(consumed);
        if self.owner.is_empty() {
            self.owner.release_empty();
        }
        mirrored
    }

    fn remaining(&self) -> usize {
        self.cursor.len()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        _: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        self.cursor
            .get(range)
            .map(wire::RetainedBytes::from)
            .ok_or(wire::RetainError::Range)
    }
}

impl<'d> wire::Cursor<'d> for bytes::Bytes<bytes::Retained> {
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.len());
        let advanced = self.try_advance(consumed);
        debug_assert!(advanced);
        consumed
    }

    fn remaining(&self) -> usize {
        self.len()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        _: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        self.clone()
            .get(range)
            .map(wire::RetainedBytes::from_buffered)
            .ok_or(wire::RetainError::Range)
    }
}

impl<'d> wire::Cursor<'d> for bytes::Bytes<bytes::Pooled<'d>> {
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.len());
        let advanced = self.try_advance(consumed);
        debug_assert!(advanced);
        consumed
    }

    fn remaining(&self) -> usize {
        self.len()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        _: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        self.clone()
            .get(range)
            .map(wire::RetainedBytes::from_pooled)
            .ok_or(wire::RetainError::Range)
    }
}

impl<'d> wire::Cursor<'d> for recv::View<'d> {
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.len());
        self.advance(consumed);
        consumed
    }

    fn remaining(&self) -> usize {
        self.len()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        let bytes = self.as_slice().get(range).ok_or(wire::RetainError::Range)?;
        wire::RetainedBytes::copy_from_slice(budget, bytes).map_err(|_| wire::RetainError::Capacity)
    }
}

impl<'d> wire::Cursor<'d> for recv::Shared<'d> {
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.len());
        self.advance(consumed);
        consumed
    }

    fn remaining(&self) -> usize {
        self.len()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        budget: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        let bytes = self
            .as_slice()
            .get(range.clone())
            .ok_or(wire::RetainError::Range)?;
        match self.accounted(range, budget) {
            Some(bytes) => Ok(wire::RetainedBytes::from_provided(bytes)),
            None => wire::RetainedBytes::copy_from_slice(budget, bytes)
                .map_err(|_| wire::RetainError::Capacity),
        }
    }
}

impl<'d> wire::Cursor<'d> for storage::Shared {
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.len());
        let advanced = self.try_advance(consumed);
        debug_assert!(advanced);
        consumed
    }

    fn remaining(&self) -> usize {
        self.len()
    }

    fn retain(
        &self,
        range: ops::Range<usize>,
        _: &resident::Budget<'d>,
    ) -> Result<wire::RetainedBytes<'d>, wire::RetainError> {
        self.get(range)
            .map(wire::RetainedBytes::from)
            .ok_or(wire::RetainError::Range)
    }
}
