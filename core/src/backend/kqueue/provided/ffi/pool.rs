use std::cell::UnsafeCell;
use std::ptr::NonNull;

use crate::io::provided::raw::buffer::BufferId;
use crate::io::provided::raw::completion::CompletedBuffer;
use crate::io::provided::raw::region::InitializedRegion;

pub(crate) struct Backing {
    storage: Box<[UnsafeCell<u8>]>,
    buf_len: usize,
}

impl Backing {
    pub(crate) fn new(entries: u16, buf_len: u32) -> Self {
        let total = (entries as usize) * (buf_len as usize);
        let storage = vec![0u8; total].into_boxed_slice();
        let storage = unsafe { Box::from_raw(Box::into_raw(storage) as *mut [UnsafeCell<u8>]) };
        Self {
            storage,
            buf_len: buf_len as usize,
        }
    }

    fn base(&self) -> NonNull<u8> {
        unsafe { NonNull::new_unchecked(UnsafeCell::raw_get(self.storage.as_ptr())) }
    }

    pub(crate) fn complete(&self, id: BufferId, len: usize) -> CompletedBuffer {
        let bid = id.into_raw();
        let ptr = unsafe { self.base().as_ptr().add(bid as usize * self.buf_len) };
        let region = unsafe {
            InitializedRegion::new(NonNull::new_unchecked(ptr), len.min(self.buf_len))
        };
        CompletedBuffer::new(id, region)
    }
}

pub(crate) struct ProvidedPool {
    base: NonNull<u8>,
    buf_len: usize,
    free: Vec<u16>,
}

pub(crate) struct TakenBuffer {
    id: BufferId,
    ptr: NonNull<u8>,
    capacity: usize,
}

impl TakenBuffer {
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn into_id(self) -> BufferId {
        self.id
    }
}

impl ProvidedPool {
    pub(crate) fn new(backing: &Backing, entries: u16) -> Self {
        let free = (0..entries).rev().collect();
        Self {
            base: backing.base(),
            buf_len: backing.buf_len,
            free,
        }
    }

    pub(crate) fn take(&mut self) -> Option<TakenBuffer> {
        let bid = self.free.pop()?;
        let ptr = unsafe { self.base.as_ptr().add(bid as usize * self.buf_len) };
        Some(TakenBuffer {
            id: unsafe { BufferId::new(bid) },
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            capacity: self.buf_len,
        })
    }

    pub(crate) fn buf_len(&self) -> usize {
        self.buf_len
    }

    pub(crate) fn defer(&mut self, id: BufferId) {
        self.free.push(id.into_raw());
    }
}
