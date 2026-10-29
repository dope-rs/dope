use std::{cell, marker, ptr};

use crate::{
    core::driver::schedule::ready::wake,
    wait::{Queue, Slot},
};

/// The queue or owning slot one linked entry registers its wake with.
pub(super) struct Active<'d> {
    pub(super) target: Target<'d>,
    pub(super) wake: wake::Waker<'d>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(super) struct Target<'d> {
    pub(super) pointer: ptr::NonNull<()>,
    driver: marker::PhantomData<cell::Cell<&'d ()>>,
}

impl<'d> Target<'d> {
    pub(super) fn queue(queue: &Queue<'d>) -> Self {
        Self {
            pointer: ptr::NonNull::from(queue).cast(),
            driver: marker::PhantomData,
        }
    }

    pub(super) fn slot(slot: &Slot<'d>) -> Self {
        Self {
            pointer: ptr::NonNull::from(slot)
                .cast::<()>()
                .map_addr(|addr| addr | 1),
            driver: marker::PhantomData,
        }
    }
}
