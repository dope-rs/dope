use std::{io, process};

use o3::collections::batch::set;

use crate::{
    backend::uring::{self, engine::lifecycle},
    driver::route,
    io::fd::handles,
};

pub(super) struct Maintenance {
    deferred_close: set::Set,
    slots: usize,
}

impl Maintenance {
    pub(super) fn new(capacity: usize) -> io::Result<Self> {
        let entries = capacity.checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope-uring: lifecycle maintenance capacity overflow",
            )
        })?;
        Ok(Self {
            deferred_close: set::Set::try_with_capacity(entries)?,
            slots: capacity,
        })
    }

    pub(super) fn schedule_close(&mut self, work: super::CloseWork) {
        let index = work.slot().raw() as usize + if work.retires_slot() { self.slots } else { 0 };
        if !self.deferred_close.insert(index) {
            process::abort();
        }
    }

    pub(super) fn pending(&self) -> bool {
        !self.deferred_close.is_empty()
    }

    pub(super) fn maintain_one(
        &mut self,
        push: impl FnOnce(super::Maintenance) -> Result<(), super::Maintenance>,
    ) -> uring::MaintenanceStep {
        let Some(work) = self.pop_close() else {
            process::abort();
        };
        if let Err(super::Maintenance::Close(work)) = push(super::Maintenance::Close(work)) {
            self.schedule_close(work);
            return uring::MaintenanceStep::Blocked;
        }
        uring::MaintenanceStep::Progress
    }

    pub(super) fn pop_close(&mut self) -> Option<lifecycle::CloseWork> {
        let index = self.deferred_close.pop()?;
        if index < self.slots {
            let slot = route::SlotIndex::from_bounded(index as u32);
            Some(lifecycle::CloseWork::untracked(
                handles::FixedSlot::from_index(slot),
            ))
        } else {
            let raw = index - self.slots;
            let slot = route::SlotIndex::from_bounded(raw as u32);
            Some(lifecycle::CloseWork::retired(
                handles::FixedSlot::from_index(slot),
            ))
        }
    }
}
