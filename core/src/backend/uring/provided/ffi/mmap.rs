use std::io::{self, Error, ErrorKind};
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

pub(in crate::backend::uring::provided) struct Mmap {
    ptr: NonNull<u8>,
    len: usize,
    _thread: ThreadBound,
}

impl Mmap {
    pub(in crate::backend::uring::provided) fn new_zeroed(len: usize) -> io::Result<Self> {
        if len == 0 || len > isize::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap length out of range",
            ));
        }
        // SAFETY: anonymous mapping with no fd; len was validated above.
        let raw = unsafe {
            mmap(
                null_mut(),
                len,
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
            // SAFETY: raw came from mmap with this exact len and is not used again.
            unsafe { munmap(raw, len) };
            return Err(Error::other("mmap returned null"));
        };
        // SAFETY: raw/len describe the mapping created above.
        unsafe { madvise(raw, len, MADV_HUGEPAGE) };
        Ok(Self {
            ptr,
            len,
            _thread: ThreadBound::NEW,
        })
    }

    pub(in crate::backend::uring::provided) fn prewarm(&mut self) {
        // SAFETY: self.ptr/self.len describe our live mapping.
        if unsafe {
            madvise(
                self.ptr.as_ptr().cast(),
                self.len,
                MADV_POPULATE_WRITE,
            ) == 0
        } {
            return;
        }
        let page = Self::page_size().unwrap_or(MIN_PAGE_ALIGN);
        for offset in (0..self.len).step_by(page) {
            // SAFETY: offset < self.len keeps the pointer inside the mapping.
            let ptr = unsafe { self.ptr.as_ptr().add(offset) };
            // SAFETY: ptr is valid for reads and writes; we hold &mut self.
            let value = unsafe { ptr.read_volatile() };
            unsafe { ptr.write_volatile(value) };
        }
    }

    pub(in crate::backend::uring::provided) fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub(in crate::backend::uring::provided) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
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

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: ptr/len describe the mapping we own; nothing touches it afterwards.
        unsafe { munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}
