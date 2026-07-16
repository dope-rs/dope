use std::fmt::{Debug, Formatter, Result};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;

use o3::marker::ThreadBound;

use crate::backend::Backend;
use crate::driver::DriverRef;

#[derive(Clone, Copy, Debug)]
pub struct FdSlot(u32, ThreadBound);

impl FdSlot {
    pub fn new(index: u32) -> Self {
        Self(index, ThreadBound::NEW)
    }

    pub fn raw(self) -> u32 {
        self.0
    }
}

pub struct Fd<'d> {
    slot: FdSlot,
    driver: DriverRef<'d>,
}

pub struct FdGuard<'a, 'd> {
    backend: &'a mut Backend,
    slot: FdSlot,
    driver: DriverRef<'d>,
    _invariant: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl Debug for Fd<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result {
        formatter.debug_tuple("Fd").field(&self.slot.0).finish()
    }
}

impl<'d> Fd<'d> {
    /// # Safety
    /// `slot` must be reserved from `driver` and uniquely owned.
    #[inline]
    pub unsafe fn from_raw_slot(slot: FdSlot, driver: DriverRef<'d>) -> Self {
        Self { slot, driver }
    }

    pub fn slot(&self) -> FdSlot {
        self.slot
    }

    #[inline]
    pub fn index(&self) -> u32 {
        self.slot.0
    }

    pub fn driver(&self) -> DriverRef<'d> {
        self.driver
    }

    pub(crate) fn into_parts(self) -> (FdSlot, DriverRef<'d>) {
        (self.slot, self.driver)
    }
}

impl<'a, 'd> FdGuard<'a, 'd> {
    pub(crate) fn new(backend: &'a mut Backend, slot: FdSlot, driver: DriverRef<'d>) -> Self {
        Self {
            backend,
            slot,
            driver,
            _invariant: PhantomData,
        }
    }

    pub fn slot(&self) -> FdSlot {
        self.slot
    }

    pub fn persist(self) -> Fd<'d> {
        let this = ManuallyDrop::new(self);
        Fd {
            slot: this.slot,
            driver: this.driver,
        }
    }
}

impl Drop for FdGuard<'_, '_> {
    fn drop(&mut self) {
        self.backend.close_fd(self.slot);
    }
}
