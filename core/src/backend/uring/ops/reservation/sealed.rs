use std::{io, mem, os::fd};

use crate::{
    backend::{self, fixed, uring::descriptor},
    driver,
    io::fd::handles,
};

pub(in crate::backend::uring::ops) struct Vacant<'a, 'd> {
    backend: &'a mut backend::Uring,
    slot: mem::ManuallyDrop<fixed::Slot<'d>>,
}

impl<'a, 'd> Vacant<'a, 'd> {
    pub(super) fn new(backend: &'a mut backend::Uring, slot: fixed::Slot<'d>) -> Self {
        Self {
            backend,
            slot: mem::ManuallyDrop::new(slot),
        }
    }

    pub(super) fn install(&mut self, file: fd::BorrowedFd<'_>) -> io::Result<()> {
        self.backend.install_reserved_file(&self.slot, file)
    }

    pub(super) fn commit(self) -> fixed::Slot<'d> {
        let mut this = mem::ManuallyDrop::new(self);
        // SAFETY: suppressing `Vacant::drop` transfers its unique slot to the
        // installed descriptor while ending the backend borrow normally.
        unsafe { mem::ManuallyDrop::take(&mut this.slot) }
    }

    pub(in crate::backend::uring::ops) fn install_handle(
        driver: &'a mut driver::Context<'_, 'd>,
        handle: descriptor::Handle,
    ) -> io::Result<handles::Descriptor<'d>> {
        let reference = driver.driver_ref();
        let slot = driver
            .backend()
            .alloc_fixed_slot(reference)
            .map_err(|error| reference.files().map_fixed_allocation_error(error))?;
        let mut vacant = Self::new(driver.backend(), slot);
        vacant.install(fd::AsFd::as_fd(handle.owned()))?;
        drop(handle);
        handles::Descriptor::from_reserved_slot(vacant.commit(), reference)
            .ok_or_else(|| io::Error::other("dope: fixed ready slot is retired"))
    }
}

impl Drop for Vacant<'_, '_> {
    fn drop(&mut self) {
        // SAFETY: Drop runs exactly once and owns the still-vacant slot.
        let slot = unsafe { mem::ManuallyDrop::take(&mut self.slot) };
        self.backend.release_vacant_slot(slot);
    }
}

const _: () = assert!(
    mem::size_of::<Vacant<'static, 'static>>()
        == mem::size_of::<(&'static mut backend::Uring, fixed::Slot<'static>)>()
);
