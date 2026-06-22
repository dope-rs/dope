use std::{
    io, ops::{Deref, DerefMut}, ptr::{NonNull, null_mut}, slice, sync::atomic::{AtomicU16, Ordering}
};

use io_uring::Submitter;
use io_uring::types::BufRingEntry;

struct Entries {
    ptr: NonNull<BufRingEntry>,
    count: u16,
    size: usize,
}

impl Entries {
    fn new(count: u16) -> io::Result<Self> {
        let size = size_of::<BufRingEntry>().checked_mul(count as usize).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "pbuf_ring size overflow")
        })?;
        // SAFETY: anonymous mmap with NULL hint + RW + MAP_PRIVATE|MAP_ANONYMOUS is page-aligned and zero-filled by the kernel.
        let raw = unsafe {
            libc::mmap(
                null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let ptr = NonNull::new(raw.cast()).expect("mmap returned non-MAP_FAILED non-null");
        Ok(Self { ptr, count, size })
    }

    fn raw_ptr(&self) -> NonNull<BufRingEntry> {
        self.ptr
    }
}

impl Deref for Entries {
    type Target = [BufRingEntry];
    fn deref(&self) -> &[BufRingEntry] {
        // SAFETY: `ptr` is a valid, page-aligned anonymous mapping with `count` zero-initialized `BufRingEntry` slots.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.count as usize) }
    }
}

impl DerefMut for Entries {
    fn deref_mut(&mut self) -> &mut [BufRingEntry] {
        // SAFETY: `ptr` is a valid, page-aligned anonymous mapping with `count` zero-initialized `BufRingEntry` slots; exclusive access via `&mut self`.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.count as usize) }
    }
}

impl Drop for Entries {
    fn drop(&mut self) {
        // SAFETY: `ptr` + `size` exactly match the anonymous mapping created in `Entries::new`.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.size); }
    }
}

struct Buffers {
    mem: Box<[u8]>,
    buf_len: usize,
}

impl Buffers {
    fn new(count: u16, buf_len: usize) -> io::Result<Self> {
        let total = (count as usize).checked_mul(buf_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "pbuf backing size overflow")
        })?;
        Ok(Self {
            mem: vec![0u8; total].into_boxed_slice(),
            buf_len,
        })
    }

    fn buf_len(&self) -> usize {
        self.buf_len
    }

    fn addr(&self, bid: u16) -> u64 {
        self.mem.as_ptr() as u64 + (bid as usize * self.buf_len) as u64
    }

    unsafe fn slice<'a>(&self, bid: u16, len: usize) -> &'a [u8] {
        let len = len.min(self.buf_len);
        // SAFETY: per fn contract; `len <= buf_len` clamped; `'a` is caller-chosen.
        unsafe {
            slice::from_raw_parts(
                self.mem.as_ptr().add(bid as usize * self.buf_len),
                len,
            )
        }
    }
}

pub(super) struct Ring {
    tail_pos: u16,
    last_published: u16,
    mask: u16,
    buffers: Buffers,
    entries: Entries,
}

impl Ring {
    const BGID: u16 = 1;

    pub(super) fn new(submitter: &Submitter<'_>, entries: u16, buf_len: usize) -> io::Result<Self> {
        if !entries.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pbuf_ring entries must be power-of-two",
            ));
        }

        let mut entries_mem = Entries::new(entries)?;
        let buffers = Buffers::new(entries, buf_len)?;

        for (bid, e) in entries_mem.iter_mut().enumerate() {
            let bid = bid as u16;
            let addr = buffers.addr(bid);
            e.set_addr(addr);
            e.set_len(buf_len as u32);
            e.set_bid(bid);
        }

        let ring = Self {
            tail_pos: entries,
            last_published: entries,
            mask: entries.wrapping_sub(1),
            buffers,
            entries: entries_mem,
        };
        ring.store_tail(entries);

        // SAFETY: `entries` is a valid, page-aligned anonymous mapping with `entries` BufRingEntry slots; lifetime managed by Entries::drop; register failure auto-drops via `?`.
        unsafe {
            submitter.register_buf_ring_with_flags(
                ring.entries.raw_ptr().as_ptr() as u64,
                entries,
                Self::BGID,
                0,
            )?;
        }

        Ok(ring)
    }

    pub(super) fn group(&self) -> u16 {
        Self::BGID
    }

    pub(super) unsafe fn slice<'a>(&self, bid: u16, len: usize) -> &'a [u8] {
        // SAFETY: `bid & mask` is within [0, count); caller guarantees buffer is held until release.
        unsafe { self.buffers.slice(bid & self.mask, len) }
    }

    pub(super) fn defer(&mut self, bid: u16) {
        let bid = bid & self.mask;
        let slot = (self.tail_pos & self.mask) as usize;
        let addr = self.buffers.addr(bid);
        let buf_len = self.buffers.buf_len() as u32;
        // SAFETY: `slot = tail_pos & mask` is within [0, count) = entries.len().
        let e = unsafe { self.entries.get_unchecked_mut(slot) };
        e.set_addr(addr);
        e.set_len(buf_len);
        e.set_bid(bid);
        self.tail_pos = self.tail_pos.wrapping_add(1);
    }

    pub(super) fn flush(&mut self) {
        if self.tail_pos == self.last_published {
            return;
        }
        self.store_tail(self.tail_pos);
        self.last_published = self.tail_pos;
    }

    fn store_tail(&self, value: u16) {
        // SAFETY: `entries` is a valid buf ring; `::tail` returns a non-null offset within it; Release synchronizes with the kernel.
        let tail = unsafe { BufRingEntry::tail(self.entries.raw_ptr().as_ptr() as *const _) as *const AtomicU16 };
        unsafe { (*tail).store(value, Ordering::Release) };
    }
}
