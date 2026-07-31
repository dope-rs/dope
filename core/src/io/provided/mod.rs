use std::cell::Cell;

use crate::driver::{DriverContext, DriverRef};

use self::raw::buffer::BufferId;
use self::raw::completion::CompletedBuffer;
use self::raw::region::InitializedRegion;

pub(crate) mod raw;

pub struct ProvidedSpan {
    offset: usize,
    len: usize,
}

pub struct ProvidedLease<'d> {
    driver: DriverRef<'d>,
    id: Cell<Option<BufferId>>,
    region: InitializedRegion,
}

pub struct ProvidedView<'d> {
    _lease: ProvidedLease<'d>,
    region: InitializedRegion,
}

impl<'d> ProvidedLease<'d> {
    pub(crate) fn from_completion(driver: DriverRef<'d>, completed: CompletedBuffer) -> Self {
        let CompletedBuffer { id, region } = completed;
        Self {
            driver,
            id: Cell::new(Some(id)),
            region,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.region.as_mut_slice()
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            self.region.advance(count),
            "dope: provided lease advance out of bounds"
        );
    }

    pub fn range_of(&self, bytes: &[u8]) -> Option<ProvidedSpan> {
        let base = self.region.ptr.as_ptr().addr();
        let start = bytes.as_ptr().addr();
        let offset = start.checked_sub(base)?;
        self.span(offset, bytes.len())
    }

    pub fn span(&self, offset: usize, len: usize) -> Option<ProvidedSpan> {
        if offset > self.region.len || len > self.region.len - offset {
            return None;
        }
        Some(ProvidedSpan { offset, len })
    }

    pub fn into_view(self, span: ProvidedSpan) -> Result<ProvidedView<'d>, Self> {
        let Some(region) = self.region.subregion(span.offset, span.len) else {
            return Err(self);
        };
        Ok(ProvidedView {
            _lease: self,
            region,
        })
    }

    pub fn release(&self, driver: &mut DriverContext<'_, 'd>) {
        let _ = self.driver;
        if let Some(id) = self.id.take() {
            driver.release_buffer(id);
        }
    }
}

impl AsRef<[u8]> for ProvidedLease<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for ProvidedLease<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.driver.return_buffer(id);
        }
    }
}

impl ProvidedView<'_> {
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn len(&self) -> usize {
        self.region.len()
    }

    pub fn is_empty(&self) -> bool {
        self.region.len() == 0
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            self.region.advance(count),
            "dope: provided view advance out of bounds"
        );
    }
}

impl AsRef<[u8]> for ProvidedView<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
