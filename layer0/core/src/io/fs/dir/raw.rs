use std::{os::fd::OwnedFd, ptr};

use o3::{alloc, mem};

pub(super) struct Header {
    count: mem::raw::Count,
    pub(super) file: OwnedFd,
}

impl Header {
    pub(super) fn try_new(file: OwnedFd) -> Result<mem::raw::Owner<Self>, alloc::Exhausted> {
        let header: Box<Self> = alloc::BoxExt::try_new(Self {
            count: mem::raw::Count::one(),
            file,
        })?;
        // SAFETY: the allocation transfers its one initialized counted edge.
        Ok(unsafe { mem::raw::Owner::from_raw(ptr::NonNull::from(Box::leak(header))) })
    }
}

// SAFETY: every owner refers to this header's stable, private count.
unsafe impl mem::raw::Edge for Header {
    fn count(&self) -> &mem::raw::Count {
        &self.count
    }
}

// SAFETY: the last edge transfers the original Box and its owned descriptor.
unsafe impl mem::raw::Target for Header {
    unsafe fn destroy(pointer: ptr::NonNull<Self>) {
        // SAFETY: the transferred `Box` frees this exact header and its owned descriptor.
        unsafe { drop(Box::from_raw(pointer.as_ptr())) };
    }
}
