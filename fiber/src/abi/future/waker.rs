use core::task::{RawWaker, RawWakerVTable};

use crate::Context;

pub(super) static VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_raw, wake_raw, wake_raw, drop_raw);

unsafe fn clone_raw(_: *const ()) -> RawWaker {
    panic!("fiber waker cannot escape its poll")
}

unsafe fn wake_raw(task: *const ()) {
    unsafe { Context::wake_raw(task) };
}

unsafe fn drop_raw(_: *const ()) {}
