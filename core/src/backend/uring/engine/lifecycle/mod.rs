use std::io;

use crate::{
    backend::{
        fixed,
        uring::{self, engine::controls},
    },
    driver::{self, route},
    io::fd::handles,
};

mod maintenance;
mod sealed;

pub(in crate::backend::uring) use sealed::RetireWork;

const RETIRE_SLOT: u32 = 1 << route::SLOT_BITS;

/// One-shot ownership of a fixed-file close transaction.
/// The slot bits name the file; the next bit carries allocator retirement.
#[must_use = "fixed-file close work must be submitted or restored to the lifecycle queue"]
#[repr(transparent)]
pub(in crate::backend::uring) struct CloseWork(u32);

const _: () = assert!(std::mem::size_of::<CloseWork>() == std::mem::size_of::<u32>());

impl CloseWork {
    fn close(close: driver::Close<'_>) -> Self {
        Self::untracked(close.into_slot())
    }

    pub(in crate::backend::uring) fn untracked(slot: handles::FixedSlot) -> Self {
        Self(slot.raw())
    }

    pub(in crate::backend::uring) fn retire(slot: fixed::Slot<'_>) -> Self {
        Self::retired(slot.retire().into_raw().into_fixed())
    }

    pub(super) fn retired(slot: handles::FixedSlot) -> Self {
        Self(slot.raw() | RETIRE_SLOT)
    }

    pub(in crate::backend::uring) fn completed_close(transaction: controls::Close) -> Self {
        Self(transaction.slot().raw())
    }

    pub(in crate::backend::uring) fn slot(&self) -> handles::FixedSlot {
        let raw = self.0 & route::SLOT_MASK as u32;
        handles::FixedSlot::from_index(route::SlotIndex::from_bounded(raw))
    }

    pub(in crate::backend::uring) fn retires_slot(&self) -> bool {
        self.0 & RETIRE_SLOT != 0
    }

    pub(in crate::backend::uring) fn into_retire(self) -> Result<RetireWork, Self> {
        if self.retires_slot() {
            Ok(RetireWork::new(self))
        } else {
            Err(self)
        }
    }
}

pub(in crate::backend::uring) enum Maintenance {
    Close(CloseWork),
}

/// Kernel lifecycle transactions only. Live fixed-file ownership is tracked by
/// the affine driver handles, never mirrored in this backend.
pub(crate) struct Table {
    maintenance: maintenance::Maintenance,
}

impl Table {
    pub(in crate::backend::uring) fn new(close_capacity: usize) -> io::Result<Self> {
        Ok(Self {
            maintenance: maintenance::Maintenance::new(close_capacity)?,
        })
    }

    pub(in crate::backend::uring) fn has_maintenance(&self) -> bool {
        self.maintenance.pending()
    }

    pub(in crate::backend::uring) fn maintain_one(
        &mut self,
        push: impl FnOnce(Maintenance) -> Result<(), Maintenance>,
    ) -> uring::MaintenanceStep {
        self.maintenance.maintain_one(push)
    }

    pub(in crate::backend::uring) fn close(
        &mut self,
        close: driver::Close<'_>,
        mut push: impl FnMut(CloseWork) -> Result<(), CloseWork>,
    ) {
        let work = CloseWork::close(close);
        if self.maintenance.pending() {
            self.maintenance.schedule_close(work);
            return;
        }
        if let Err(work) = push(work) {
            self.maintenance.schedule_close(work);
        }
    }

    pub(in crate::backend::uring) fn stage_close(&mut self, close: driver::Close<'_>) {
        self.maintenance.schedule_close(CloseWork::close(close));
    }

    pub(in crate::backend::uring) fn stage_retire(&mut self, slot: fixed::Slot<'_>) {
        self.maintenance.schedule_close(CloseWork::retire(slot));
    }

    pub(in crate::backend::uring) fn restore(&mut self, work: CloseWork) {
        self.maintenance.schedule_close(work);
    }

    pub(in crate::backend::uring) fn pop_close(&mut self) -> Option<CloseWork> {
        self.maintenance.pop_close()
    }
}
