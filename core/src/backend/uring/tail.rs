use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, Ordering};

use o3::marker::ThreadBound;

pub(super) struct Tail {
    ptr: NonNull<AtomicU16>,
    _thread: ThreadBound,
}

impl Tail {
    pub(super) unsafe fn new(ptr: *mut u16) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr.cast()) },
            _thread: ThreadBound::NEW,
        }
    }

    pub(super) fn publish(&self, value: u16) {
        unsafe { self.ptr.as_ref() }.store(value, Ordering::Release);
    }
}
