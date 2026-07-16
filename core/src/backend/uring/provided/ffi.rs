use std::io::{self, Error, ErrorKind};
use std::ptr::{NonNull, null_mut};

use o3::marker::ThreadBound;

const MIN_PAGE_ALIGN: usize = 4096;
const MAP_FLAGS: libc::c_int = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;

pub(super) struct Mmap {
    ptr: NonNull<u8>,
    len: usize,
    _thread: ThreadBound,
}

impl Mmap {
    pub(super) fn new_zeroed(len: usize) -> io::Result<Self> {
        if len == 0 || len > isize::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap length out of range",
            ));
        }
        // SAFETY: anonymous mapping with no fd; len was validated above.
        let raw = unsafe {
            libc::mmap(
                null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                MAP_FLAGS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }
        let Some(ptr) = NonNull::new(raw.cast()) else {
            // SAFETY: raw came from mmap with this exact len and is not used again.
            unsafe { libc::munmap(raw, len) };
            return Err(Error::other("mmap returned null"));
        };
        // SAFETY: raw/len describe the mapping created above.
        unsafe { libc::madvise(raw, len, libc::MADV_HUGEPAGE) };
        Ok(Self {
            ptr,
            len,
            _thread: ThreadBound::NEW,
        })
    }

    pub(super) fn prewarm(&mut self) {
        // SAFETY: self.ptr/self.len describe our live mapping.
        if unsafe {
            libc::madvise(
                self.ptr.as_ptr().cast(),
                self.len,
                libc::MADV_POPULATE_WRITE,
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

    pub(super) fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub(super) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn page_size() -> io::Result<usize> {
        // SAFETY: sysconf takes no pointer arguments.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        usize::try_from(page)
            .ok()
            .filter(|page| page.is_power_of_two())
            .ok_or_else(Error::last_os_error)
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: ptr/len describe the mapping we own; nothing touches it afterwards.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}
