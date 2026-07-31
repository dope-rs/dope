use std::fmt::{Debug, Formatter, Result};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;

use o3::marker::ThreadBound;

use crate::backend::Backend;
use crate::driver::DriverRef;
use crate::driver::ready::ReadyHandle;
use crate::driver::token::SlotIndex;

#[derive(Clone, Copy, Debug)]
pub struct FdSlot(SlotIndex, ThreadBound);

impl FdSlot {
    pub(crate) const fn from_index(index: SlotIndex) -> Self {
        Self(index, ThreadBound::NEW)
    }

    pub(crate) const fn try_from_raw(raw: u32) -> Option<Self> {
        match SlotIndex::try_new(raw) {
            Some(index) => Some(Self::from_index(index)),
            None => None,
        }
    }

    pub fn raw(self) -> u32 {
        self.0.raw()
    }

    pub(crate) fn token_index(self) -> SlotIndex {
        self.0
    }
}

#[repr(transparent)]
pub struct AcceptedSlot<'d> {
    slot: FdSlot,
    _brand: PhantomData<fn(&'d ()) -> &'d ()>,
}

pub struct Fd<'d> {
    slot: FdSlot,
    driver: DriverRef<'d>,
    retirement: Retirement,
}

pub struct FdGuard<'a, 'd> {
    backend: &'a mut Backend,
    slot: FdSlot,
    driver: DriverRef<'d>,
    retire_slot: bool,
    _invariant: PhantomData<fn(&'d ()) -> &'d ()>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Retirement {
    Range,
    Slot,
    Done,
}

impl Debug for Fd<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result {
        formatter.debug_tuple("Fd").field(&self.slot.raw()).finish()
    }
}

impl<'d> AcceptedSlot<'d> {
    pub(crate) fn from_completion(slot: FdSlot, _driver: DriverRef<'d>) -> Self {
        Self {
            slot,
            _brand: PhantomData,
        }
    }

    pub fn bind(self, driver: DriverRef<'d>) -> Fd<'d> {
        Fd {
            slot: self.slot,
            driver,
            retirement: Retirement::Range,
        }
    }
}

impl<'d> Fd<'d> {
    pub(crate) fn from_reserved_slot(slot: FdSlot, driver: DriverRef<'d>) -> Self {
        Self {
            slot,
            driver,
            retirement: Retirement::Slot,
        }
    }

    pub(crate) fn from_range_slot(slot: FdSlot, driver: DriverRef<'d>) -> Self {
        Self {
            slot,
            driver,
            retirement: Retirement::Range,
        }
    }

    pub fn slot(&self) -> FdSlot {
        self.slot
    }

    pub fn index(&self) -> u32 {
        self.slot.raw()
    }

    pub fn token_index(&self) -> SlotIndex {
        self.slot.token_index()
    }

    pub fn driver(&self) -> DriverRef<'d> {
        self.driver
    }

    pub fn ready_handle(&self) -> ReadyHandle<'d> {
        self.driver.fixed_ready(self.slot)
    }

    pub(crate) fn into_parts(self) -> (FdSlot, DriverRef<'d>, bool) {
        (self.slot, self.driver, self.retirement == Retirement::Slot)
    }

    pub(crate) fn retire_slot(&mut self, driver: DriverRef<'d>) -> Option<FdSlot> {
        assert!(
            self.driver == driver,
            "dope: fixed fd retired through a different driver"
        );
        if self.retirement != Retirement::Slot {
            return None;
        }
        self.retirement = Retirement::Done;
        Some(self.slot)
    }
}

impl<'a, 'd> FdGuard<'a, 'd> {
    pub(crate) fn new(
        backend: &'a mut Backend,
        slot: FdSlot,
        driver: DriverRef<'d>,
        retire_slot: bool,
    ) -> Self {
        Self {
            backend,
            slot,
            driver,
            retire_slot,
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
            retirement: if this.retire_slot {
                Retirement::Slot
            } else {
                Retirement::Range
            },
        }
    }
}

impl Drop for FdGuard<'_, '_> {
    fn drop(&mut self) {
        self.backend.close_fd(self.slot);
        if self.retire_slot {
            self.backend.retire_fixed_range(self.slot.raw(), 1);
        }
    }
}
