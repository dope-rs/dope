use std::{marker, pin};

use dope::core::driver::{
    self,
    schedule::{self},
};
use o3::collections::{self, fixed::pinned};

use crate::{context, wait::sealed};

/// Owns one pinned waiter registry behind a safe operational API.
#[repr(transparent)]
pub struct Queue<'d> {
    registry: pin::Pin<Box<sealed::Registry<'d>>>,
}

impl<'d> Queue<'d> {
    pub fn try_with_capacity(
        driver: driver::Reference<'d>,
        capacity: usize,
    ) -> Result<Self, collections::AllocationError> {
        let registry =
            collections::BoxExt::try_box(sealed::Registry::with_capacity(driver, capacity))?;
        Ok(Self {
            registry: Box::into_pin(registry),
        })
    }

    pub fn try_register<'target, 'poll>(
        &'target self,
        waiter: pin::Pin<&sealed::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> bool {
        self.registry.as_ref().try_register(waiter, context)
    }

    pub fn wake(&self, work: schedule::Application<'_, 'd>) -> WakeStatus {
        self.registry.as_ref().wake(work)
    }

    pub fn wake_one(&self, work: schedule::Application<'_, 'd>) -> WakeStatus {
        self.registry.as_ref().wake_one(work)
    }

    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }
}

#[must_use = "Pending means the requested wake was not fully admitted in this application turn"]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeStatus {
    Complete,
    Pending,
}

/// Owns a fixed dense table of independently bounded waiter registries.
#[repr(transparent)]
pub struct Table<'d> {
    registries: pinned::Slice<sealed::Registry<'d>>,
}

/// Owns a fixed dense table of single-waiter registration slots.
#[repr(transparent)]
pub struct Slots<'d> {
    slots: pinned::Slice<sealed::Slot>,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> Table<'d> {
    pub fn try_with_capacity(
        driver: driver::Reference<'d>,
        slots: usize,
        waiter_capacity: usize,
    ) -> Result<Self, collections::AllocationError> {
        let registries: Box<[sealed::Registry<'d>]> =
            collections::BoxSliceExt::try_box_with(slots, |_| {
                sealed::Registry::with_capacity(driver, waiter_capacity)
            })?;
        Ok(Self {
            registries: registries.into(),
        })
    }

    pub fn try_register<'target, 'poll>(
        &'target self,
        index: usize,
        waiter: pin::Pin<&sealed::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> bool {
        self.registries
            .get(index)
            .is_some_and(|registry| registry.try_register(waiter, context))
    }

    pub fn wake(&self, index: usize, work: schedule::Application<'_, 'd>) -> WakeStatus {
        if let Some(registry) = self.registries.get(index) {
            registry.wake(work)
        } else {
            WakeStatus::Complete
        }
    }

    pub fn wake_one(&self, index: usize, work: schedule::Application<'_, 'd>) -> WakeStatus {
        if let Some(registry) = self.registries.get(index) {
            registry.wake_one(work)
        } else {
            WakeStatus::Complete
        }
    }

    pub fn is_empty(&self, index: usize) -> bool {
        self.registries
            .get(index)
            .is_none_or(|registry| registry.is_empty())
    }
}

impl<'d> Slots<'d> {
    pub fn try_with_capacity(
        _driver: driver::Reference<'d>,
        capacity: usize,
    ) -> Result<Self, collections::AllocationError> {
        let slots: Box<[sealed::Slot]> =
            collections::BoxSliceExt::try_box_with(capacity, |_| sealed::Slot::new())?;
        Ok(Self {
            slots: slots.into(),
            _driver: marker::PhantomData,
        })
    }

    pub fn try_register<'target, 'poll>(
        &'target self,
        index: usize,
        waiter: pin::Pin<&sealed::Waiter<'target, 'd>>,
        context: pin::Pin<&context::Context<'poll, 'd>>,
    ) -> bool {
        self.slots
            .get(index)
            .is_some_and(|slot| slot.try_register(waiter, context))
    }

    pub fn wake(&self, index: usize) {
        if let Some(slot) = self.slots.get(index) {
            slot.wake();
        }
    }

    pub fn clear(&self, index: usize) {
        if let Some(slot) = self.slots.get(index) {
            slot.clear();
        }
    }

    pub fn clear_all(&self) {
        for slot in self.slots.iter() {
            slot.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}
