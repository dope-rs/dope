use std::{io, ptr::NonNull};

use io_uring::Submitter;
use io_uring::types::BufRingEntry;
use std::io::{Error, ErrorKind};

use crate::io::provided::raw::buffer::BufferId;
use crate::io::provided::raw::completion::CompletedBuffer;
use crate::io::provided::raw::region::InitializedRegion;

use super::mmap::Mmap;
use super::tail::Tail;
use std::slice::from_raw_parts_mut;

struct Entries {
    mem: Mmap,
    count: u16,
}

impl Entries {
    fn new(count: u16) -> io::Result<Self> {
        let size = size_of::<BufRingEntry>()
            .checked_mul(count as usize)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "pbuf_ring size overflow"))?;
        Ok(Self {
            mem: Mmap::new_zeroed(size)?,
            count,
        })
    }

    fn raw_ptr(&self) -> NonNull<BufRingEntry> {
        unsafe { NonNull::new_unchecked(self.mem.as_ptr().cast_mut().cast()) }
    }

    fn as_mut_slice(&mut self) -> &mut [BufRingEntry] {
        unsafe { from_raw_parts_mut(self.mem.as_mut_ptr().cast(), self.count as usize) }
    }

    unsafe fn get_unchecked_mut(&mut self, index: usize) -> &mut BufRingEntry {
        unsafe { self.as_mut_slice().get_unchecked_mut(index) }
    }
}

struct Buffers {
    mem: Mmap,
    buf_len: usize,
}

impl Buffers {
    fn new(count: u16, buf_len: usize) -> io::Result<Self> {
        let total = (count as usize)
            .checked_mul(buf_len)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "pbuf backing size overflow"))?;
        let mut mem = Mmap::new_zeroed(total)?;
        mem.prewarm();
        Ok(Self { mem, buf_len })
    }

    fn buf_len(&self) -> usize {
        self.buf_len
    }

    fn addr(&self, bid: u16) -> u64 {
        self.mem.as_ptr() as u64 + (bid as usize * self.buf_len) as u64
    }

    fn region(&self, bid: u16, len: usize) -> InitializedRegion {
        let len = len.min(self.buf_len);
        let ptr = unsafe {
            NonNull::new_unchecked(
                self.mem
                    .as_ptr()
                    .add(bid as usize * self.buf_len)
                    .cast_mut(),
            )
        };
        unsafe { InitializedRegion::new(ptr, len) }
    }
}

pub(crate) struct ProvidedRing {
    tail_pos: u16,
    last_published: u16,
    mask: u16,
    tail: Tail,
    buffers: Buffers,
    entries: Entries,
}

impl ProvidedRing {
    pub(crate) const BGID: u16 = 1;

    pub(crate) fn new(submitter: &Submitter<'_>, entries: u16, buf_len: usize) -> io::Result<Self> {
        if !entries.is_power_of_two() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "pbuf_ring entries must be power-of-two",
            ));
        }

        let mut entries_mem = Entries::new(entries)?;
        let buffers = Buffers::new(entries, buf_len)?;

        for (bid, e) in entries_mem.as_mut_slice().iter_mut().enumerate() {
            let bid = bid as u16;
            let addr = buffers.addr(bid);
            e.set_addr(addr);
            e.set_len(buf_len as u32);
            e.set_bid(bid);
        }

        let tail = unsafe {
            Tail::new(BufRingEntry::tail(entries_mem.raw_ptr().as_ptr() as *const _) as *mut u16)
        };
        let ring = Self {
            tail_pos: entries,
            last_published: entries,
            mask: entries.wrapping_sub(1),
            tail,
            buffers,
            entries: entries_mem,
        };
        ring.store_tail(entries);

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

    pub(crate) fn buf_len(&self) -> usize {
        self.buffers.buf_len()
    }

    pub(crate) fn complete(&self, bid: u16, len: usize) -> CompletedBuffer {
        let bid = bid & self.mask;
        let id = unsafe { BufferId::new(bid) };
        CompletedBuffer::new(id, self.buffers.region(bid, len))
    }

    pub(crate) fn defer(&mut self, id: BufferId) {
        self.defer_raw(id.into_raw());
    }

    pub(crate) fn defer_completion(&mut self, bid: u16) {
        self.defer_raw(bid);
    }

    fn defer_raw(&mut self, bid: u16) {
        let bid = bid & self.mask;
        let slot = (self.tail_pos & self.mask) as usize;
        let addr = self.buffers.addr(bid);
        let buf_len = self.buffers.buf_len() as u32;
        let e = unsafe { self.entries.get_unchecked_mut(slot) };
        e.set_addr(addr);
        e.set_len(buf_len);
        e.set_bid(bid);
        self.tail_pos = self.tail_pos.wrapping_add(1);
    }

    pub(crate) fn flush(&mut self) {
        if self.tail_pos == self.last_published {
            return;
        }
        self.store_tail(self.tail_pos);
        self.last_published = self.tail_pos;
    }

    fn store_tail(&self, value: u16) {
        self.tail.publish(value);
    }
}
