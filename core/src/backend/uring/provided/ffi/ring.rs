use std::io::{self, Error, ErrorKind};
use std::mem::{align_of, offset_of, size_of};
use std::num::NonZeroU32;
use std::process::abort;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::{IoUring, Submitter};
use io_uring::types::BufRingEntry;

use crate::io::recv::completion::Completion;
use crate::io::recv::raw::Region;

use super::mapping::Mapping;

#[repr(C)]
#[derive(Clone, Copy)]
struct RingEntry {
    addr: u64,
    len: u32,
    bid: u16,
    tail: u16,
}

const _: () = {
    assert!(size_of::<RingEntry>() == size_of::<BufRingEntry>());
    assert!(align_of::<RingEntry>() == align_of::<BufRingEntry>());
    assert!(offset_of!(RingEntry, addr) == 0);
    assert!(offset_of!(RingEntry, len) == 8);
    assert!(offset_of!(RingEntry, bid) == 12);
    assert!(offset_of!(RingEntry, tail) == 14);
    assert!(size_of::<AtomicU16>() == size_of::<u16>());
    assert!(align_of::<AtomicU16>() == align_of::<u16>());
};

impl RingEntry {
    fn new(addr: u64, len: u32, bid: u16) -> Self {
        Self {
            addr,
            len,
            bid,
            tail: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct Layout {
    mask: u16,
    buf_len: NonZeroU32,
}

impl Layout {
    fn new(entries: u16, buf_len: usize) -> io::Result<Self> {
        if !entries.is_power_of_two() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "pbuf_ring entries must be power-of-two",
            ));
        }
        let buf_len = u32::try_from(buf_len)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "provided buffer length out of range",
                )
            })?;
        Ok(Self {
            mask: entries - 1,
            buf_len,
        })
    }

    fn entries(self) -> u16 {
        self.mask + 1
    }

    fn slot(self, raw: u16) -> u16 {
        raw & self.mask
    }

    fn buffer_len(self) -> u32 {
        self.buf_len.get()
    }

    fn offset(self, slot: u16) -> usize {
        slot as usize * self.buffer_len() as usize
    }

    fn addr(self, base: *const u8, slot: u16) -> u64 {
        base as u64 + self.offset(slot) as u64
    }
}

struct Storage {
    entries: Mapping<RingEntry>,
    buffers: Mapping<u8>,
    layout: Layout,
}

impl Storage {
    fn new(entries: u16, buf_len: usize) -> io::Result<Self> {
        let layout = Layout::new(entries, buf_len)?;
        let total = (layout.entries() as usize)
            .checked_mul(layout.buffer_len() as usize)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "pbuf backing size overflow"))?;
        let mut buffers = Mapping::new_zeroed(total)?;
        buffers.prewarm();

        let base = buffers.as_ptr();
        let entries = Mapping::new_with(layout.entries() as usize, |raw| {
            let slot = raw as u16;
            RingEntry::new(layout.addr(base, slot), layout.buffer_len(), slot)
        })?;

        Ok(Self {
            entries,
            buffers,
            layout,
        })
    }

    fn write(&mut self, position: u16, buffer: Buffer) {
        let position = self.layout.slot(position);
        let bid = buffer.raw();
        // SAFETY: Layout masks every slot below the Mapping length created
        // from that same layout. Only these payload fields are written before
        // the release-store publishes them to the kernel; tail is not borrowed.
        unsafe {
            let entry = self.entries.as_mut_ptr().add(position as usize);
            (&raw mut (*entry).addr).write(self.layout.addr(self.buffers.as_ptr(), bid));
            (&raw mut (*entry).len).write(self.layout.buffer_len());
            (&raw mut (*entry).bid).write(bid);
        }
    }

    fn complete(&self, raw: u16, len: usize) -> Completion {
        let buffer = self.buffer_from_cqe(raw);
        let slot = buffer.raw();
        // SAFETY: slot is below layout.entries(), and buffers contains that many
        // strides. The buffer-selected CQE initialized the reported prefix.
        let region = unsafe {
            Region::new(
                self.buffers.as_non_null().add(self.layout.offset(slot)),
                len.min(self.layout.buffer_len() as usize),
            )
        };
        Completion::new(buffer, region)
    }

    fn buffer_from_cqe(&self, raw: u16) -> Buffer {
        let slot = self.layout.slot(raw);
        // SAFETY: a buffer-selected CQE transfers this live slot from the
        // kernel to this ring. Layout masks the ABI value to a live slot.
        unsafe { Buffer::from_cqe(slot) }
    }

    fn publish(&self, value: u16) {
        // SAFETY: entries is a non-empty live mapping; the ABI assertions prove
        // its first tail field has AtomicU16 size and alignment.
        unsafe {
            let entry = self.entries.as_ptr().cast_mut();
            AtomicU16::from_ptr(&raw mut (*entry).tail).store(value, Ordering::Release);
        }
    }

    fn entries(&self) -> u16 {
        self.layout.entries()
    }

    fn ring_addr(&self) -> u64 {
        self.entries.as_ptr() as u64
    }

    fn buf_len(&self) -> usize {
        self.layout.buffer_len() as usize
    }
}

/// Owns the kernel ring together with the userspace memory registered into it.
/// Drop revokes the registration before either field can release its storage.
pub(crate) struct RegisteredRing {
    io: IoUring,
    provided: ProvidedRing,
}

pub(crate) struct Buffer(u16);

impl Buffer {
    /// # Safety
    /// `slot` is a live buffer selected by a CQE, whose ownership has just
    /// transferred from the kernel to this driver.
    unsafe fn from_cqe(slot: u16) -> Self {
        Self(slot)
    }

    fn raw(&self) -> u16 {
        self.0
    }
}

pub(crate) struct ProvidedRing {
    tail_pos: u16,
    last_published: u16,
    storage: Storage,
}

impl RegisteredRing {
    pub(crate) const BGID: u16 = 1;

    pub(crate) fn new(io: IoUring, entries: u16, buf_len: usize) -> io::Result<Self> {
        let provided = ProvidedRing::new(&io.submitter(), entries, buf_len)?;
        Ok(Self { io, provided })
    }

    pub(crate) fn io(&self) -> &IoUring {
        &self.io
    }

    pub(crate) fn io_mut(&mut self) -> &mut IoUring {
        &mut self.io
    }

    pub(crate) fn provided(&self) -> &ProvidedRing {
        &self.provided
    }

    pub(crate) fn provided_mut(&mut self) -> &mut ProvidedRing {
        &mut self.provided
    }

    pub(crate) fn split(&mut self) -> (&mut IoUring, &mut ProvidedRing) {
        (&mut self.io, &mut self.provided)
    }
}

impl Drop for RegisteredRing {
    fn drop(&mut self) {
        // Unmapping after a failed unregister would violate the kernel ABI.
        if self
            .io
            .submitter()
            .unregister_buf_ring(Self::BGID)
            .is_err()
        {
            abort();
        }
    }
}

impl ProvidedRing {
    fn new(submitter: &Submitter<'_>, entries: u16, buf_len: usize) -> io::Result<Self> {
        let storage = Storage::new(entries, buf_len)?;
        let ring = Self {
            tail_pos: entries,
            last_published: entries,
            storage,
        };
        ring.store_tail(entries);

        unsafe {
            submitter.register_buf_ring_with_flags(
                ring.storage.ring_addr(),
                ring.storage.entries(),
                RegisteredRing::BGID,
                0,
            )?;
        }

        Ok(ring)
    }

    pub(crate) fn buf_len(&self) -> usize {
        self.storage.buf_len()
    }

    pub(crate) fn entries(&self) -> usize {
        self.storage.entries() as usize
    }

    pub(crate) fn complete(&self, bid: u16, len: usize) -> Completion {
        self.storage.complete(bid, len)
    }

    pub(crate) fn buffer_from_cqe(&self, bid: u16) -> Buffer {
        self.storage.buffer_from_cqe(bid)
    }

    pub(crate) fn defer(&mut self, buffer: Buffer) {
        self.storage.write(self.tail_pos, buffer);
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
        self.storage.publish(value);
    }
}
