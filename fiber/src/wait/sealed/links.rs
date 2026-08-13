use std::{pin, ptr};

use o3::cell;

use crate::wait;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct WaiterLink(cell::StableLink<wait::Waiter<'static, 'static>>);

impl WaiterLink {
    pub(super) fn get(&self) -> pin::Pin<&wait::Waiter<'static, 'static>> {
        self.0.get()
    }
}

struct Source<T>(ptr::NonNull<T>);

// SAFETY: Source exists only for a live bidirectional registration. The
// invariant target lifetime keeps the endpoint live until unlinking.
unsafe impl<T> cell::raw::StableLinkSource<T> for Source<T> {
    fn pointer(self) -> ptr::NonNull<T> {
        self.0
    }
}

pub(super) enum WaiterLinks {}

impl WaiterLinks {
    pub(super) fn from_waiter(waiter: pin::Pin<&wait::Waiter<'_, '_>>) -> WaiterLink {
        WaiterLink(cell::StableLink::from_stable(Source(
            ptr::NonNull::from(waiter.get_ref()).cast::<wait::Waiter<'static, 'static>>(),
        )))
    }
}
