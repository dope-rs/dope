use std::{io, os::fd};

use crate::{
    backend::{fixed, kqueue::descriptor},
    driver::{
        self,
        route::{self, table},
        settings,
    },
    io::fd::handles,
};

pub(crate) struct Files {
    table: Vec<Option<descriptor::Handle>>,
    accept: AcceptSlots,
    slots: fixed::Slots,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct AcceptIndex(route::SlotIndex);

struct AcceptSlots {
    free: Vec<AcceptIndex>,
    limit: table::Capacity,
}

/// A short-lived reservation which returns its slot unless a live descriptor is inserted.
pub(in crate::backend::kqueue::engine) struct AcceptVacancy<'a> {
    files: &'a mut Files,
    index: AcceptIndex,
    release: bool,
}

impl AcceptSlots {
    fn new(limit: table::Capacity) -> io::Result<Self> {
        let mut free = Vec::new();
        free.try_reserve_exact(limit.get()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "dope: accept slot allocator storage unavailable",
            )
        })?;
        free.extend(limit.slots().rev().map(AcceptIndex));
        Ok(Self { free, limit })
    }

    fn take(&mut self) -> Option<AcceptIndex> {
        self.free.pop()
    }

    fn release(&mut self, index: AcceptIndex) {
        assert!(
            index.0.raw() < self.limit.raw() && self.free.len() < self.limit.get(),
            "dope-kqueue: invalid accept slot release"
        );
        self.free.push(index);
    }

    const fn contains(&self, index: u32) -> bool {
        index < self.limit.raw()
    }
}

impl AcceptVacancy<'_> {
    pub(in crate::backend::kqueue::engine) fn insert(
        mut self,
        fd: descriptor::Handle,
    ) -> handles::Accepted {
        let index = self.index;
        let entry = &mut self.files.table[index.0.raw() as usize];
        assert!(entry.is_none(), "dope-kqueue: accept slot already live");
        *entry = Some(fd);
        self.release = false;
        handles::Accepted::from_live(handles::FixedSlot::from_index(index.0))
    }
}

impl Drop for AcceptVacancy<'_> {
    fn drop(&mut self) {
        if self.release {
            self.files.accept.release(self.index);
        }
    }
}

impl Files {
    pub(in crate::backend::kqueue) fn new(layout: settings::FileSlots) -> io::Result<Self> {
        use std::iter;
        let capacity = layout.table_capacity().get();
        let slots = fixed::Slots::new(layout)?;
        Ok(Self {
            table: iter::repeat_with(|| None).take(capacity).collect(),
            accept: AcceptSlots::new(layout.accept_capacity())?,
            slots,
        })
    }

    pub(in crate::backend::kqueue) fn take_index(&mut self, index: usize) -> Option<fd::OwnedFd> {
        let fd = self.table.get_mut(index).and_then(Option::take)?;
        if let Some(index) = self.accept.limit.slot(index) {
            self.accept.release(AcceptIndex(index));
        }
        Some(fd.into())
    }

    pub(in crate::backend::kqueue) fn alloc_slots<'d>(
        &mut self,
        len: u32,
        driver: driver::Reference<'d>,
    ) -> io::Result<fixed::Reservation<'d>> {
        self.slots.alloc(len, driver)
    }

    pub(in crate::backend::kqueue) fn alloc<'d>(
        &mut self,
        driver: driver::Reference<'d>,
    ) -> io::Result<fixed::Slot<'d>> {
        self.slots.alloc_slot(driver)
    }

    pub(in crate::backend::kqueue) fn retire(&mut self, slots: fixed::Reservation<'_>) {
        self.slots.release(slots);
    }

    pub(in crate::backend::kqueue) fn retire_slot(&mut self, slot: fixed::Slot<'_>) {
        self.slots.release_slot(slot);
    }

    pub(in crate::backend::kqueue) fn install_outbound(
        &mut self,
        slot: handles::FixedSlot,
        fd: descriptor::Handle,
    ) {
        let index = slot.raw() as usize;
        assert!(
            !self.accept.contains(slot.raw()) && index < self.table.len(),
            "dope-kqueue: fixed-file slot was not reserved for outbound use"
        );
        let entry = &mut self.table[index];
        assert!(entry.is_none(), "dope-kqueue: outbound slot already live");
        *entry = Some(fd);
    }

    pub(in crate::backend::kqueue) fn raw(&self, slot: handles::FixedSlot) -> Option<fd::RawFd> {
        self.table
            .get(slot.raw() as usize)
            .and_then(Option::as_ref)
            .map(|fd| fd::AsRawFd::as_raw_fd(fd.owned()))
    }

    pub(in crate::backend::kqueue) fn borrow(
        &self,
        slot: handles::FixedSlot,
    ) -> Option<fd::BorrowedFd<'_>> {
        self.table
            .get(slot.raw() as usize)
            .and_then(Option::as_ref)
            .map(|fd| fd::AsFd::as_fd(fd.owned()))
    }

    pub(in crate::backend::kqueue::engine) fn vacant_accept(
        &mut self,
    ) -> Option<AcceptVacancy<'_>> {
        let index = self.accept.take()?;
        Some(AcceptVacancy {
            files: self,
            index,
            release: true,
        })
    }
}
