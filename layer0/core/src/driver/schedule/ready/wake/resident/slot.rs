use std::{cell, mem};

use crate::driver::{self, schedule::ready};

/// Lifetime-free storage used only inside a driver-owned resident container.
#[derive(Default)]
#[repr(transparent)]
pub(in crate::driver) struct Slot {
    target: cell::Cell<Option<ready::Stored>>,
}

impl Slot {
    pub(in crate::driver) fn set(&self, target: ready::Retained<'_>) {
        self.target.set(Some(target.raw()));
    }

    pub(in crate::driver) fn clear(&self) {
        self.target.set(None);
    }

    pub(in crate::driver) fn wake_once<'d>(&self, driver: driver::Reference<'d>) {
        let Some(stored) = self.target.take() else {
            return;
        };
        let key = ready::Access::new(&driver).bind(stored);
        ready::Retained::new(key).wake(driver);
    }

    pub(in crate::driver) fn wake<'d>(&self, driver: driver::Reference<'d>) {
        let Some(stored) = self.target.get() else {
            return;
        };
        let key = ready::Access::new(&driver).bind(stored);
        ready::Retained::new(key).wake(driver);
    }
}

const _: () = assert!(mem::size_of::<Slot>() == mem::size_of::<u64>());
