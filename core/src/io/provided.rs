use std::cell::Cell;
use std::ptr::NonNull;
use std::slice;

use crate::driver::buffers::ProvidedBuffers;
use crate::driver::{DriverContext, DriverRef};

pub struct ProvidedLease<'d> {
    driver: DriverRef<'d>,
    bid: Cell<Option<u16>>,
    ptr: NonNull<u8>,
    len: usize,
}

pub struct ProvidedView<'d> {
    _lease: ProvidedLease<'d>,
    ptr: NonNull<u8>,
    len: usize,
}

impl<'d> ProvidedLease<'d> {
    /// # Safety
    /// `bid` must be a uniquely owned live completion buffer. This call
    /// transfers its ownership to the returned lease.
    pub unsafe fn from_completion(driver: &mut DriverContext<'_, 'd>, len: u32, bid: u16) -> Self {
        let reference = driver.driver_ref();
        let (ptr, len) = unsafe { driver.buffer_ptr_len(len, bid) };
        Self {
            driver: reference,
            bid: Cell::new(Some(bid)),
            ptr,
            len,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn range_of(&self, bytes: &[u8]) -> Option<(usize, usize)> {
        let base = self.ptr.as_ptr().addr();
        let start = bytes.as_ptr().addr();
        let offset = start.checked_sub(base)?;
        (offset <= self.len && bytes.len() <= self.len - offset).then_some((offset, bytes.len()))
    }

    pub fn into_view(self, offset: usize, len: usize) -> ProvidedView<'d> {
        assert!(offset <= self.len && len <= self.len - offset);
        let ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(offset)) };
        ProvidedView {
            _lease: self,
            ptr,
            len,
        }
    }

    /// Transfers buffer-return responsibility to a retained subrange.
    ///
    /// The completion buffer has one logical owner. This method moves that
    /// ownership into the returned view without moving the lease value, which
    /// permits receive values borrowing the buffer to be dropped first.
    pub fn retained_view(&self, offset: usize, len: usize) -> ProvidedView<'d> {
        assert!(offset <= self.len && len <= self.len - offset);
        let bid = self
            .bid
            .take()
            .expect("provided buffer ownership was already transferred");
        let ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(offset)) };
        ProvidedView {
            _lease: Self {
                driver: self.driver,
                bid: Cell::new(Some(bid)),
                ptr: self.ptr,
                len: self.len,
            },
            ptr,
            len,
        }
    }

    pub fn release(&self, driver: &mut DriverContext<'_, 'd>) {
        let _ = self.driver;
        if let Some(bid) = self.bid.take() {
            unsafe { driver.release(bid) };
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
        if let Some(bid) = self.bid.take() {
            self.driver.return_buffer(bid);
        }
    }
}

impl ProvidedView<'_> {
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn advance(&mut self, count: usize) {
        assert!(
            count <= self.len,
            "dope: provided view advance out of bounds"
        );
        self.ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(count)) };
        self.len -= count;
    }
}

impl AsRef<[u8]> for ProvidedView<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
