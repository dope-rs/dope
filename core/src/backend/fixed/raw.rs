use std::{marker, mem};

use crate::{backend::fixed, driver, io::fd::handles};

#[repr(transparent)]
pub(crate) struct Retirement(handles::FixedSlot);

impl Retirement {
    pub(super) fn new(slot: handles::FixedSlot) -> Self {
        Self(slot)
    }

    pub(crate) unsafe fn from_deferred(slot: handles::FixedSlot) -> Self {
        Self(slot)
    }

    pub(crate) fn into_fixed(self) -> handles::FixedSlot {
        self.0
    }

    pub(crate) fn bind<'d>(self, _driver: driver::Reference<'d>) -> fixed::Retirement<'d> {
        fixed::Retirement {
            slot: self.0,
            _brand: marker::PhantomData,
        }
    }
}

const _: () = assert!(mem::size_of::<Retirement>() == mem::size_of::<handles::FixedSlot>());
