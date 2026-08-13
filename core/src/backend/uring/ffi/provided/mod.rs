use std::{io, mem, num, slice, sync::atomic};

use io_uring::types;

use crate::{
    driver::settings,
    io::{datagram, recv},
};

mod mapping;

#[repr(C)]
#[derive(Clone, Copy)]
struct RingEntry {
    addr: u64,
    len: u32,
    bid: u16,
    tail: u16,
}

const _: () = {
    assert!(mem::size_of::<RingEntry>() == mem::size_of::<types::BufRingEntry>());
    assert!(mem::align_of::<RingEntry>() == mem::align_of::<types::BufRingEntry>());
    assert!(mem::offset_of!(RingEntry, addr) == 0);
    assert!(mem::offset_of!(RingEntry, len) == 8);
    assert!(mem::offset_of!(RingEntry, bid) == 12);
    assert!(mem::offset_of!(RingEntry, tail) == 14);
    assert!(mem::size_of::<atomic::AtomicU16>() == mem::size_of::<u16>());
    assert!(mem::align_of::<atomic::AtomicU16>() == mem::align_of::<u16>());
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
    buf_len: num::NonZeroU32,
}

impl Layout {
    fn new(receive: settings::Receive) -> Self {
        Self {
            mask: receive.entries() - 1,
            buf_len: receive.nonzero_buffer_len(),
        }
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
    entries: mapping::Mapping<RingEntry>,
    buffers: mapping::Populated<u8>,
    layout: Layout,
}

impl Storage {
    fn new(receive: settings::Receive) -> io::Result<Self> {
        use mapping::Mapping;
        let layout = Layout::new(receive);
        let total = receive.backing_bytes();
        let buffers = Mapping::new_zeroed(total)?.populate()?;

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

    fn region(&self, buffer: &Buffer, len: usize) -> recv::raw::Region {
        let slot = buffer.raw();
        // SAFETY: slot is below layout.entries(), and buffers contains that many
        // strides. The buffer-selected CQE initialized the reported prefix.
        unsafe {
            use crate::io::recv::raw::Region;
            Region::new(
                self.buffers.as_non_null().add(self.layout.offset(slot)),
                len.min(self.layout.buffer_len() as usize),
            )
        }
    }

    fn bytes(&self, buffer: &Buffer, len: usize) -> &[u8] {
        let slot = buffer.raw();
        let len = len.min(self.layout.buffer_len() as usize);
        // SAFETY: slot is below layout.entries(), buffers owns every stride,
        // and a selected completion initialized the reported prefix.
        unsafe { slice::from_raw_parts(self.buffers.as_ptr().add(self.layout.offset(slot)), len) }
    }

    fn buffer_from_completion(&self, raw: u16) -> Buffer {
        let slot = self.layout.slot(raw);
        // SAFETY: a buffer-selected CQE transfers this live slot from the
        // kernel to this ring. Layout masks the ABI value to a live slot.
        unsafe { Buffer::from_cqe(slot) }
    }

    fn contains(&self, raw: u16) -> bool {
        raw < self.layout.entries()
    }

    fn publish(&self, value: u16) {
        // SAFETY: entries is a non-empty live mapping; the ABI assertions prove
        // its first tail field has AtomicU16 size and alignment.
        unsafe {
            use std::sync::atomic::{AtomicU16, Ordering};
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

#[repr(transparent)]
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

/// Startup buffer proof whose ring borrow keeps kernel-visible storage alive.
/// Failed unregistration retains the mapping rather than exposing freed memory.
pub(in crate::backend::uring) struct CanaryRing<'ring> {
    ring: &'ring mut io_uring::IoUring,
    storage: mem::ManuallyDrop<Storage>,
    tail_pos: u16,
    registered: bool,
}

impl ProvidedRing {
    pub(crate) const GROUP_ID: u16 = 1;

    pub(in crate::backend::uring) fn new(
        submitter: &io_uring::Submitter<'_>,
        receive: settings::Receive,
    ) -> io::Result<Self> {
        let entries = receive.entries();
        let storage = Storage::new(receive)?;
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
                Self::GROUP_ID,
                0,
            )?;
        }

        Ok(ring)
    }

    pub(crate) fn region(&self, buffer: &Buffer, len: usize) -> recv::raw::Region {
        self.storage.region(buffer, len)
    }

    pub(crate) fn buffer_from_completion(&self, bid: u16) -> Buffer {
        self.storage.buffer_from_completion(bid)
    }

    pub(crate) fn defer(&mut self, buffer: Buffer) {
        self.storage.write(self.tail_pos, buffer);
        self.tail_pos = self.tail_pos.wrapping_add(1);
    }

    pub(in crate::backend::uring) fn flush(&mut self) {
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

impl<'ring> CanaryRing<'ring> {
    pub(in crate::backend::uring) const GROUP_ID: u16 = 2;

    pub(in crate::backend::uring) fn new(ring: &'ring mut io_uring::IoUring) -> io::Result<Self> {
        let receive = settings::Receive::fixed::<2, { datagram::SlotLen::MIN_BYTES }>();
        let storage = Storage::new(receive)?;
        storage.publish(storage.entries());
        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                storage.ring_addr(),
                storage.entries(),
                Self::GROUP_ID,
                0,
            )?;
        }
        Ok(Self {
            ring,
            tail_pos: storage.entries(),
            storage: mem::ManuallyDrop::new(storage),
            registered: true,
        })
    }

    pub(in crate::backend::uring) fn ring(&mut self) -> &mut io_uring::IoUring {
        self.ring
    }

    pub(in crate::backend::uring) fn inspect<T>(
        &mut self,
        bid: u16,
        len: usize,
        inspect: impl FnOnce(&[u8]) -> io::Result<T>,
    ) -> io::Result<T> {
        if !self.storage.contains(bid) {
            return Err(io::Error::other(
                "dope: io_uring canary selected an invalid buffer ID",
            ));
        }
        let buffer = self.storage.buffer_from_completion(bid);
        let result = if len <= self.storage.buf_len() {
            inspect(self.storage.bytes(&buffer, len))
        } else {
            Err(io::Error::other(
                "dope: io_uring canary exceeded its provided buffer",
            ))
        };
        self.storage.write(self.tail_pos, buffer);
        self.tail_pos = self.tail_pos.wrapping_add(1);
        self.storage.publish(self.tail_pos);
        result
    }

    pub(in crate::backend::uring) fn finish(mut self) -> io::Result<()> {
        self.unregister()
    }

    fn unregister(&mut self) -> io::Result<()> {
        if !self.registered {
            return Ok(());
        }
        self.ring.submitter().unregister_buf_ring(Self::GROUP_ID)?;
        self.registered = false;
        unsafe { mem::ManuallyDrop::drop(&mut self.storage) };
        Ok(())
    }
}

impl Drop for CanaryRing<'_> {
    fn drop(&mut self) {
        let _ = self.unregister();
    }
}
