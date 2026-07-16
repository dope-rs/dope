use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static COUNT: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}

pub struct TrackingAlloc;

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn record(bytes: usize) {
    COUNT.with(|c| c.set(c.get() + 1));
    BYTES.with(|b| b.set(b.get() + bytes));
}

pub fn allocations_during(f: impl FnOnce()) -> (usize, usize) {
    let (count, bytes) = (COUNT.with(Cell::get), BYTES.with(Cell::get));
    f();
    (COUNT.with(Cell::get) - count, BYTES.with(Cell::get) - bytes)
}
