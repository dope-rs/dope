use std::ptr::NonNull;
use std::slice::from_raw_parts;

pub(crate) struct InitializedRegion {
    pub(in crate::io::provided) ptr: NonNull<u8>,
    pub(in crate::io::provided) len: usize,
}

impl InitializedRegion {
    /// # Safety
    /// `ptr` must address `len` initialized bytes that remain readable while
    /// this region or any region derived from it is live.
    pub(crate) const unsafe fn new(ptr: NonNull<u8>, len: usize) -> Self {
        Self { ptr, len }
    }

    pub(in crate::io::provided) fn as_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub(in crate::io::provided) fn subregion(&self, offset: usize, len: usize) -> Option<Self> {
        if offset > self.len || len > self.len - offset {
            return None;
        }
        Some(Self {
            ptr: unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(offset)) },
            len,
        })
    }

    pub(in crate::io::provided) fn len(&self) -> usize {
        self.len
    }

    pub(in crate::io::provided) fn advance(&mut self, count: usize) -> bool {
        if count > self.len {
            return false;
        }
        self.ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(count)) };
        self.len -= count;
        true
    }
}
