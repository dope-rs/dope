use std::cell::UnsafeCell;
use std::ptr::NonNull;

pub(crate) struct Backing {
    storage: Box<[UnsafeCell<u8>]>,
    buf_len: usize,
}

impl Backing {
    pub(super) fn new(entries: u16, buf_len: u32) -> Self {
        let total = (entries as usize) * (buf_len as usize);
        let storage = vec![0u8; total].into_boxed_slice();
        let storage = unsafe { Box::from_raw(Box::into_raw(storage) as *mut [UnsafeCell<u8>]) };
        Self {
            storage,
            buf_len: buf_len as usize,
        }
    }

    fn base(&self) -> NonNull<u8> {
        unsafe { NonNull::new_unchecked(UnsafeCell::raw_get(self.storage.as_ptr())) }
    }

    /// # Safety
    /// `bid` is checked out and contains initialized bytes.
    pub(crate) unsafe fn ptr_len(&self, bid: u16, len: usize) -> (NonNull<u8>, usize) {
        let ptr = unsafe { self.base().as_ptr().add(bid as usize * self.buf_len) };
        (
            unsafe { NonNull::new_unchecked(ptr) },
            len.min(self.buf_len),
        )
    }
}

pub(crate) struct Provided {
    base: NonNull<u8>,
    buf_len: usize,
    free: Vec<u16>,
}

impl Provided {
    pub(super) fn new(backing: &Backing, entries: u16) -> Self {
        let free = (0..entries).rev().collect();
        Self {
            base: backing.base(),
            buf_len: backing.buf_len,
            free,
        }
    }

    pub(super) fn take(&mut self) -> Option<(u16, *mut u8, usize)> {
        let bid = self.free.pop()?;
        let ptr = unsafe { self.base.as_ptr().add(bid as usize * self.buf_len) };
        Some((bid, ptr, self.buf_len))
    }

    pub(crate) fn buf_len(&self) -> usize {
        self.buf_len
    }

    pub(crate) fn defer(&mut self, bid: u16) {
        self.free.push(bid);
    }
}
