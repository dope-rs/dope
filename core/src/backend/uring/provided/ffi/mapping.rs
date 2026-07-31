use std::io::{self, Error, ErrorKind};
use std::mem::size_of;
use std::ptr::{NonNull, null_mut};

use o3::marker::ThreadBound;
use libc::MADV_HUGEPAGE;
use libc::MADV_POPULATE_WRITE;
use libc::MAP_ANONYMOUS;
use libc::MAP_FAILED;
use libc::MAP_NORESERVE;
use libc::MAP_PRIVATE;
use libc::PROT_READ;
use libc::PROT_WRITE;
use libc::_SC_PAGESIZE;
use libc::c_int;
use libc::madvise;
use libc::mmap;
use libc::munmap;
use libc::sysconf;

const MIN_PAGE_ALIGN: usize = 4096;
const MAP_FLAGS: c_int = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;

pub(super) struct Mapping<T: Copy> {
    ptr: NonNull<T>,
    len: usize,
    _thread: ThreadBound,
}

impl<T: Copy> Mapping<T> {
    pub(super) fn new_with(len: usize, mut init: impl FnMut(usize) -> T) -> io::Result<Self> {
        let mapping = Self::allocate(len)?;
        for index in 0..len {
            // SAFETY: allocate reserved len properly aligned T slots, each index
            // is visited exactly once, and Copy excludes destructors on failure.
            unsafe { mapping.ptr.as_ptr().add(index).write(init(index)) };
        }
        Ok(mapping)
    }

    fn allocate(len: usize) -> io::Result<Self> {
        let byte_len = size_of::<T>()
            .checked_mul(len)
            .filter(|&len| len != 0 && len <= isize::MAX as usize)
            .ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "mmap element count out of range")
            })?;
        if align_of::<T>() > MIN_PAGE_ALIGN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap element alignment exceeds page alignment",
            ));
        }
        // SAFETY: anonymous mapping with no fd; byte_len was validated above.
        let raw = unsafe {
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
            return Err(Error::last_os_error());
        }
        let Some(ptr) = NonNull::new(raw.cast()) else {
            // SAFETY: raw came from mmap with this exact byte length and is not used again.
            unsafe { munmap(raw, byte_len) };
            return Err(Error::other("mmap returned null"));
        };
        // SAFETY: raw/byte_len describe the mapping created above.
        unsafe { madvise(raw, byte_len, MADV_HUGEPAGE) };
        Ok(Self {
            ptr,
            len,
            _thread: ThreadBound::NEW,
        })
    }

    pub(super) fn prewarm(&mut self) {
        let byte_len = self.byte_len();
        let base = self.ptr.cast::<u8>();
        // SAFETY: base/byte_len describe our live mapping.
        if unsafe {
            madvise(
                base.as_ptr().cast(),
                byte_len,
                MADV_POPULATE_WRITE,
            ) == 0
        } {
            return;
        }
        let page = Self::page_size().unwrap_or(MIN_PAGE_ALIGN);
        for offset in (0..byte_len).step_by(page) {
            // SAFETY: offset < byte_len keeps ptr inside the exclusively borrowed
            // mapping, where the byte is valid for volatile reads and writes.
            unsafe {
                let ptr = base.as_ptr().add(offset);
                ptr.write_volatile(ptr.read_volatile());
            }
        }
    }

    pub(super) fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> NonNull<T> {
        self.ptr
    }

    pub(super) fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    fn byte_len(&self) -> usize {
        size_of::<T>() * self.len
    }

    fn page_size() -> io::Result<usize> {
        // SAFETY: sysconf takes no pointer arguments.
        let page = unsafe { sysconf(_SC_PAGESIZE) };
        usize::try_from(page)
            .ok()
            .filter(|page| page.is_power_of_two())
            .ok_or_else(Error::last_os_error)
    }
}

impl Mapping<u8> {
    pub(super) fn new_zeroed(len: usize) -> io::Result<Self> {
        // Linux anonymous mappings are zero-filled, and every u8 bit pattern is valid.
        Self::allocate(len)
    }
}

impl<T: Copy> Drop for Mapping<T> {
    fn drop(&mut self) {
        // SAFETY: ptr/len describe the mapping we own; nothing touches it afterwards.
        unsafe { munmap(self.ptr.as_ptr().cast(), self.byte_len()) };
    }
}
