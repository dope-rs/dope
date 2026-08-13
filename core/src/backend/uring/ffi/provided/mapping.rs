use std::{io, mem, ptr};

const MIN_PAGE_ALIGN: usize = 4096;
const MAP_FLAGS: libc::c_int = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;

/// Thread-affine virtual storage reserved from the process address space.
pub(super) struct Mapping<T> {
    ptr: ptr::NonNull<T>,
    len: usize,
    _thread: o3::ThreadBound,
}

/// Mapping whose writable pages were admitted and populated by the kernel.
#[repr(transparent)]
pub(super) struct Populated<T>(Mapping<T>);

const _: () = {
    assert!(mem::size_of::<Populated<u8>>() == mem::size_of::<Mapping<u8>>());
    assert!(mem::align_of::<Populated<u8>>() == mem::align_of::<Mapping<u8>>());
};

impl<T> Mapping<T> {
    fn allocate(len: usize) -> io::Result<Self> {
        use std::io::ErrorKind;

        use libc::MAP_FAILED;
        use o3::ThreadBound;
        let byte_len = mem::size_of::<T>()
            .checked_mul(len)
            .filter(|&len| len != 0 && len <= isize::MAX as usize)
            .ok_or_else(|| {
                io::Error::new(ErrorKind::InvalidInput, "mmap element count out of range")
            })?;
        if mem::align_of::<T>() > MIN_PAGE_ALIGN {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "mmap element alignment exceeds page alignment",
            ));
        }
        // SAFETY: anonymous mapping with no fd; byte_len was validated above.
        let raw = unsafe {
            use std::ptr::null_mut;

            use libc::{PROT_READ, PROT_WRITE, mmap};
            mmap(
                null_mut(),
                byte_len,
                PROT_READ | PROT_WRITE,
                MAP_FLAGS,
                -1,
                0,
            )
        };
        if raw == MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let Some(ptr) = ptr::NonNull::new(raw.cast()) else {
            // SAFETY: raw came from mmap with this exact byte length and is not used again.
            unsafe { libc::munmap(raw, byte_len) };
            return Err(io::Error::other("mmap returned null"));
        };
        Ok(Self {
            ptr,
            len,
            _thread: ThreadBound::NEW,
        })
    }

    pub(super) fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> ptr::NonNull<T> {
        self.ptr
    }

    pub(super) fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    fn byte_len(&self) -> usize {
        mem::size_of::<T>() * self.len
    }

    fn advise_huge_pages(&self) {
        unsafe {
            use libc::MADV_HUGEPAGE;
            libc::madvise(self.ptr.as_ptr().cast(), self.byte_len(), MADV_HUGEPAGE);
        }
    }

    pub(super) fn populate(self) -> io::Result<Populated<T>> {
        let result = unsafe {
            use libc::MADV_POPULATE_WRITE;
            libc::madvise(
                self.ptr.as_ptr().cast(),
                self.byte_len(),
                MADV_POPULATE_WRITE,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Populated(self))
    }
}

impl<T: Copy> Mapping<T> {
    pub(super) fn new_with(len: usize, mut init: impl FnMut(usize) -> T) -> io::Result<Self> {
        let mapping = Self::allocate(len)?;
        mapping.advise_huge_pages();
        for index in 0..len {
            // SAFETY: allocate reserved len properly aligned T slots, each index
            // is visited exactly once, and Copy excludes destructors on failure.
            unsafe { mapping.ptr.as_ptr().add(index).write(init(index)) };
        }
        Ok(mapping)
    }
}

impl Mapping<u8> {
    pub(super) fn new_zeroed(len: usize) -> io::Result<Self> {
        let mapping = Self::allocate(len)?;
        mapping.advise_huge_pages();
        Ok(mapping)
    }
}

impl<T> Populated<T> {
    pub(super) fn as_ptr(&self) -> *const T {
        self.0.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> ptr::NonNull<T> {
        self.0.as_non_null()
    }
}

impl<T> Drop for Mapping<T> {
    fn drop(&mut self) {
        // SAFETY: ptr/len describe the mapping we own; nothing touches it afterwards.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.byte_len()) };
    }
}
