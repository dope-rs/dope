use std::{marker, mem, pin, ptr};

use ready::task;

use crate::driver::{route, schedule::ready};

#[derive(Clone, Copy)]
struct Pointer(ptr::NonNull<()>);

#[derive(Clone, Copy)]
union Repr {
    dispatch: route::Token,
    task: Pointer,
    free: ready::FreeLink,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Payload(Repr);

pub(in crate::driver::schedule::ready) struct Task<'d> {
    payload: Payload,
    _driver: marker::PhantomData<fn(task::Node<'d>) -> task::Node<'d>>,
}

impl Payload {
    pub(in crate::driver::schedule::ready) fn free(next: ready::FreeLink) -> Self {
        Self(Repr { free: next })
    }

    pub(in crate::driver::schedule::ready) fn dispatch(target: route::Token) -> Self {
        Self(Repr { dispatch: target })
    }

    fn task(node: pin::Pin<&task::Node<'_>>) -> Self {
        Self(Repr {
            task: Pointer(ptr::NonNull::from(node.get_ref()).cast()),
        })
    }

    pub(in crate::driver::schedule::ready) unsafe fn into_dispatch(self) -> route::Token {
        unsafe { self.0.dispatch }
    }

    pub(in crate::driver::schedule::ready) unsafe fn into_task<'a, 'd>(
        self,
        _access: ready::Access<'a, 'd>,
    ) -> pin::Pin<&'a task::Node<'d>>
    where
        'd: 'a,
    {
        let pointer = unsafe { self.0.task }.0.cast::<task::Node<'d>>();
        unsafe { pin::Pin::new_unchecked(pointer.as_ref()) }
    }

    pub(in crate::driver::schedule::ready) unsafe fn into_free(self) -> ready::FreeLink {
        unsafe { self.0.free }
    }
}

impl<'d> Task<'d> {
    /// # Safety
    /// The node must remain pinned until the installed table entry is released.
    pub(in crate::driver::schedule::ready) unsafe fn new(node: pin::Pin<&task::Node<'d>>) -> Self {
        Self {
            payload: Payload::task(node),
            _driver: marker::PhantomData,
        }
    }

    pub(in crate::driver::schedule::ready) fn into_payload(self) -> Payload {
        self.payload
    }
}

const _: () = assert!(mem::size_of::<Payload>() == 2 * mem::size_of::<u64>());
const _: () = assert!(mem::align_of::<Payload>() == mem::align_of::<u64>());
const _: () = assert!(mem::size_of::<Task<'static>>() == 2 * mem::size_of::<u64>());
