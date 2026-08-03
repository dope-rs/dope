use std::cell::UnsafeCell;
use std::io::{self, Error, ErrorKind};
use std::ptr::NonNull;

use crate::io::recv::completion::Completion;
use crate::io::recv::raw::Region;

#[derive(Debug)]
pub(crate) struct Buffer(u16);

impl Buffer {
    fn from_free_slot(slot: u16) -> Self {
        Self(slot)
    }

    pub(crate) fn raw(&self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy)]
struct Layout {
    entries: u16,
    buf_len: usize,
    bytes: usize,
}

impl Layout {
    fn new(entries: u16, buf_len: usize) -> io::Result<Self> {
        if entries == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "receive buffer count must be nonzero",
            ));
        }
        if u32::try_from(buf_len).is_err() || buf_len == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "receive buffer length out of range",
            ));
        }
        let Some(bytes) = usize::from(entries)
            .checked_mul(buf_len)
            .filter(|&bytes| bytes <= isize::MAX as usize)
        else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "receive backing size out of range",
            ));
        };
        Ok(Self {
            entries,
            buf_len,
            bytes,
        })
    }
}

pub(crate) struct Backing {
    storage: Box<[UnsafeCell<u8>]>,
    buf_len: usize,
}

impl Backing {
    pub(crate) fn allocate(entries: u16, buf_len: usize) -> io::Result<(Self, Pool)> {
        let layout = Layout::new(entries, buf_len)?;
        let storage = vec![0u8; layout.bytes].into_boxed_slice();
        // SAFETY: UnsafeCell<u8> is repr(transparent) over u8, so this keeps
        // the exact allocation pointer, length, and layout of the Box above.
        let storage = unsafe { Box::from_raw(Box::into_raw(storage) as *mut [UnsafeCell<u8>]) };
        let backing = Self {
            storage,
            buf_len: layout.buf_len,
        };
        let pool = Pool::new(&backing, layout);
        Ok((backing, pool))
    }

    fn base(&self) -> NonNull<u8> {
        // SAFETY: Layout rejects zero entries and zero-length buffers, so the
        // backing allocation contains at least one byte and its base is live.
        unsafe { NonNull::new_unchecked(UnsafeCell::raw_get(self.storage.as_ptr())) }
    }

}

pub(crate) struct Pool {
    base: NonNull<u8>,
    buf_len: usize,
    entries: usize,
    free: Vec<Buffer>,
}

pub(crate) struct Taken {
    buffer: Buffer,
    ptr: NonNull<u8>,
    capacity: usize,
}

impl Taken {
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn into_buffer(self) -> Buffer {
        self.buffer
    }
}

impl Pool {
    fn new(backing: &Backing, layout: Layout) -> Self {
        let free = (0..layout.entries)
            .rev()
            .map(Buffer::from_free_slot)
            .collect();
        Self {
            base: backing.base(),
            buf_len: backing.buf_len,
            entries: usize::from(layout.entries),
            free,
        }
    }

    pub(crate) fn take(&mut self) -> Option<Taken> {
        let buffer = self.free.pop()?;
        let bid = buffer.raw();
        // SAFETY: free contains only Buffers created from 0..entries by new.
        // Layout checked entries * buf_len before allocating backing, so this
        // stride remains within the live backing allocation.
        let ptr = unsafe {
            self.base
                .as_ptr()
                .add(usize::from(bid) * self.buf_len)
        };
        Some(Taken {
            buffer,
            // SAFETY: the pointer above is an in-bounds offset from a live,
            // non-null backing allocation.
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            capacity: self.buf_len,
        })
    }

    pub(crate) fn buf_len(&self) -> usize {
        self.buf_len
    }

    pub(crate) fn entries(&self) -> usize {
        self.entries
    }

    pub(crate) fn complete(&self, buffer: Buffer, len: usize) -> Completion {
        let bid = buffer.raw();
        // SAFETY: Buffer is private and this Pool creates it only from
        // 0..entries of this exact allocation. Layout checked entries *
        // buf_len before allocating backing, so this stride stays in bounds.
        let ptr = unsafe {
            self.base
                .as_ptr()
                .add(usize::from(bid) * self.buf_len)
        };
        // SAFETY: ptr is the live start of Buffer's buf_len-byte region; the
        // kernel initialized at most len bytes, which is clamped below.
        let region = unsafe {
            Region::new(NonNull::new_unchecked(ptr), len.min(self.buf_len))
        };
        Completion::new(buffer, region)
    }

    pub(crate) fn defer(&mut self, buffer: Buffer) {
        self.free.push(buffer);
    }
}
