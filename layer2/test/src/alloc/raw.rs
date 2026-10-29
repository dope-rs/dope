use std::{
    alloc::{GlobalAlloc, Layout, System},
    array, ptr,
};

use o3::alloc::Exhausted;

use crate::alloc::{COUNTS, REMAINING, Tracker};

fn permit() -> Result<(), Exhausted> {
    REMAINING.with(|left| match left.get() {
        None => Ok(()),
        Some(0) => {
            left.set(None);
            Err(Exhausted)
        }
        Some(count) => {
            left.set(Some(count - 1));
            Ok(())
        }
    })
}

fn record(delta: [usize; 4]) {
    COUNTS.with(|counts| {
        let previous = counts.get();
        counts.set(array::from_fn(|index| {
            previous[index].wrapping_add(delta[index])
        }));
    });
}

// SAFETY: every admitted allocation and release preserves System's exact layout and pointer.
unsafe impl GlobalAlloc for Tracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if permit().is_err() {
            return ptr::null_mut();
        }
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record([1, layout.size(), 0, 0]);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if permit().is_err() {
            return ptr::null_mut();
        }
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record([1, layout.size(), 0, 0]);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record([0, 0, 1, layout.size()]);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, bytes: usize) -> *mut u8 {
        if permit().is_err() {
            return ptr::null_mut();
        }
        let current = unsafe { System.realloc(pointer, layout, bytes) };
        if !current.is_null() {
            record([1, bytes, 1, layout.size()]);
        }
        current
    }
}
