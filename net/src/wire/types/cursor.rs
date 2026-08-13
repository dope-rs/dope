use std::ops;

use dope_core::io::recv;
use o3::buffer::{bytes, resident, storage};

use crate::wire;

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
