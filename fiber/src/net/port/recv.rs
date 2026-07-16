use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ptr::drop_in_place;

use o3::cell::RawCell;

use crate::io::RecvBuffer;

pub(super) struct RecvSlot<'d> {
    value: RawCell<MaybeUninit<RecvBuffer<'d>>>,
    len: Cell<u32>,
    next: Cell<u32>,
}

impl<'d> RecvSlot<'d> {
    pub(super) fn new(next: u32) -> Self {
        Self {
            value: RawCell::new(MaybeUninit::uninit()),
            len: Cell::new(0),
            next: Cell::new(next),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    pub(super) fn len(&self) -> usize {
        self.len.get() as usize
    }

    pub(super) fn next(&self) -> u32 {
        self.next.get()
    }

    pub(super) fn set_next(&self, next: u32) {
        self.next.set(next);
    }

    pub(super) fn insert(&self, value: RecvBuffer<'d>, len: u32) {
        debug_assert!(self.is_empty());
        debug_assert_ne!(len, 0);
        debug_assert_eq!(value.as_slice().len(), len as usize);
        unsafe {
            self.value.with_mut(|slot| {
                slot.write(value);
            })
        };
        self.len.set(len);
    }

    pub(super) fn copy_prefix(&self, dst: &mut [u8]) {
        debug_assert!(!self.is_empty());
        debug_assert!(dst.len() <= self.len());
        unsafe {
            self.value.with(|value| {
                let src = value.assume_init_ref().as_slice();
                dst.copy_from_slice(&src[..dst.len()]);
            })
        };
    }

    pub(super) fn advance(&self, n: usize) {
        let len = self.len();
        assert!(n < len, "fiber: receive slot advance out of bounds");
        unsafe {
            self.value
                .with_mut(|value| value.assume_init_mut().advance(n))
        };
        self.len.set((len - n) as u32);
    }

    pub(super) fn take(&self) -> Option<RecvBuffer<'d>> {
        if self.len.replace(0) == 0 {
            return None;
        }
        Some(unsafe { self.value.with(|value| value.assume_init_read()) })
    }
}

impl Drop for RecvSlot<'_> {
    fn drop(&mut self) {
        if self.len.get() != 0 {
            unsafe { drop_in_place(self.value.get_mut().as_mut_ptr()) };
        }
    }
}
