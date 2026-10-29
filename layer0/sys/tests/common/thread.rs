use std::{cell::Cell, ptr::NonNull};

use dope_sys::thread::raw::Task;

pub struct Completed(pub Cell<bool>);

// SAFETY: the test retains this context at a stable address and grants only the
// worker access to the cell until native joining returns ownership to the test.
unsafe impl Task for Completed {
    unsafe fn run(context: NonNull<Self>) {
        // SAFETY: Entry admits this initialized context for this sole execution.
        unsafe { context.as_ref() }.0.set(true);
    }
}
