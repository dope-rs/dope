use std::{alloc, cell, marker};

thread_local! {
    static COUNT: cell::Cell<usize> = const { cell::Cell::new(0) };
    static BYTES: cell::Cell<usize> = const { cell::Cell::new(0) };
    static AT_LEAST: cell::Cell<usize> = const { cell::Cell::new(0) };
}

pub struct TrackingAlloc<const MINIMUM: usize = 0> {
    system: marker::PhantomData<alloc::System>,
}

impl<const MINIMUM: usize> TrackingAlloc<MINIMUM> {
    pub const fn new() -> Self {
        Self {
            system: marker::PhantomData,
        }
    }

    fn record(bytes: usize) {
        COUNT.with(|count| count.set(count.get() + 1));
        BYTES.with(|total| total.set(total.get() + bytes));
        if bytes >= MINIMUM {
            AT_LEAST.with(|count| count.set(count.get() + 1));
        }
    }

    pub fn during(f: impl FnOnce()) -> (usize, usize) {
        Self::measure(f).1
    }

    pub fn measure<T>(f: impl FnOnce() -> T) -> (T, (usize, usize)) {
        let (count, bytes) = (COUNT.with(cell::Cell::get), BYTES.with(cell::Cell::get));
        let value = f();
        (
            value,
            (
                COUNT.with(cell::Cell::get) - count,
                BYTES.with(cell::Cell::get) - bytes,
            ),
        )
    }

    pub fn at_least_during(f: impl FnOnce()) -> usize {
        let count = AT_LEAST.with(cell::Cell::get);
        f();
        AT_LEAST.with(cell::Cell::get) - count
    }
}

impl<const MINIMUM: usize> Default for TrackingAlloc<MINIMUM> {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: every operation preserves `System`'s allocation contract unchanged; tracking
// only updates non-allocating thread-local counters before forwarding the exact inputs.
unsafe impl<const MINIMUM: usize> alloc::GlobalAlloc for TrackingAlloc<MINIMUM> {
    unsafe fn alloc(&self, layout: alloc::Layout) -> *mut u8 {
        Self::record(layout.size());
        // SAFETY: the caller supplies the valid layout required by `GlobalAlloc::alloc`.
        unsafe { alloc::System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: alloc::Layout) -> *mut u8 {
        Self::record(layout.size());
        // SAFETY: the caller supplies the valid layout required by `GlobalAlloc::alloc_zeroed`.
        unsafe { alloc::System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: alloc::Layout) {
        // SAFETY: the caller guarantees that `ptr` and `layout` describe a live allocation
        // returned by this allocator, which delegates all allocations to `System`.
        unsafe { alloc::System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: alloc::Layout, new_size: usize) -> *mut u8 {
        Self::record(new_size);
        // SAFETY: the caller supplies the live `System` allocation and its original layout.
        unsafe { alloc::System.realloc(ptr, layout, new_size) }
    }
}
