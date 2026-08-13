use std::ptr;

pub(crate) struct Region {
    pub(in crate::io::recv) ptr: ptr::NonNull<u8>,
    pub(in crate::io::recv) len: usize,
}

impl Region {
    /// # Safety
    /// `ptr` must address `len` initialized bytes that remain readable while
    /// this region or any region derived from it is live.
    pub(crate) const unsafe fn new(ptr: ptr::NonNull<u8>, len: usize) -> Self {
        Self { ptr, len }
    }

    pub(in crate::io::recv) fn as_slice(&self) -> &[u8] {
        unsafe {
            use std::slice::from_raw_parts;
            from_raw_parts(self.ptr.as_ptr(), self.len)
        }
    }

    pub(in crate::io::recv) fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            use std::slice::from_raw_parts_mut;
            from_raw_parts_mut(self.ptr.as_ptr(), self.len)
        }
    }

    pub(in crate::io::recv) fn subregion(&self, offset: usize, len: usize) -> Option<Self> {
        if offset > self.len || len > self.len - offset {
            return None;
        }
        Some(Self {
            ptr: unsafe { ptr::NonNull::new_unchecked(self.ptr.as_ptr().add(offset)) },
            len,
        })
    }

    pub(in crate::io::recv) fn split_off(&mut self, mid: usize) -> Option<Self> {
        if mid > self.len {
            return None;
        }
        let region = Self {
            ptr: unsafe { ptr::NonNull::new_unchecked(self.ptr.as_ptr().add(mid)) },
            len: self.len - mid,
        };
        self.len = mid;
        Some(region)
    }

    pub(in crate::io::recv) fn len(&self) -> usize {
        self.len
    }

    pub(in crate::io::recv) fn duplicate(&self) -> Self {
        Self {
            ptr: self.ptr,
            len: self.len,
        }
    }

    pub(in crate::io::recv) fn advance(&mut self, count: usize) -> bool {
        if count > self.len {
            return false;
        }
        self.ptr = unsafe { ptr::NonNull::new_unchecked(self.ptr.as_ptr().add(count)) };
        self.len -= count;
        true
    }
}
