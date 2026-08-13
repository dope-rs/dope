pub(in crate::backend::uring) mod descriptor;
mod engine;
pub(crate) mod ops;

use std::{io, mem, os::fd, process};

use self::engine::{lifecycle, submit, tuning};
use crate::{
    backend::{self, fixed},
    driver::{self, lifecycle::routing},
    platform,
};

const _: () = {
    use crate::io::Completion;
    assert!(mem::size_of::<Completion>() == 3 * mem::size_of::<u64>());
};

pub struct Uring {
    ring: ring::Ready,
    tuning: tuning::Table,
    lifecycle: lifecycle::Table,
    reactor_cursor: bool,
    fixed_slots: fixed::Slots,
    pub(crate) routes: routing::Routes,
}

impl platform::Affinity for Uring {
    type Cpus = ffi::Cpus;
    type Binding = ffi::Cpu;
}

enum MaintenanceStep {
    Progress,
    Blocked,
}

mod capabilities;
pub(crate) mod ffi;
mod opcodes;
mod ring;
pub(crate) mod submission;

impl Uring {
    fn next_reactor_cursor(&mut self) -> usize {
        let cursor = usize::from(self.reactor_cursor);
        self.reactor_cursor = !self.reactor_cursor;
        cursor
    }

    pub(crate) fn alloc_fixed_slot<'d>(
        &mut self,
        driver: driver::Reference<'d>,
    ) -> io::Result<fixed::Slot<'d>> {
        self.fixed_slots.alloc_slot(driver)
    }

    fn has_maintenance(&self) -> bool {
        self.lifecycle.has_maintenance()
    }

    fn maintain_one(&mut self) -> MaintenanceStep {
        let Self {
            ring, lifecycle, ..
        } = self;
        lifecycle.maintain_one(|operation| match operation {
            lifecycle::Maintenance::Close(work) => {
                match submit::Writer::new(&mut *ring).try_close(work) {
                    Ok(()) => Ok(()),
                    Err(work) => Err(lifecycle::Maintenance::Close(work)),
                }
            }
        })
    }

    pub(crate) fn install_reserved_file(
        &mut self,
        slot: &fixed::Slot<'_>,
        file: fd::BorrowedFd<'_>,
    ) -> io::Result<()> {
        self.ring.install_file(slot.fixed(), file)
    }

    pub(crate) fn release_vacant_slot(&mut self, slot: fixed::Slot<'_>) {
        self.fixed_slots.release_slot(slot);
    }
}

impl backend::WakeFactory for Uring {
    fn open_blocking_wake_ends() -> io::Result<(fd::OwnedFd, fd::OwnedFd)> {
        use crate::backend::uring::ffi::pipe::Pipe;

        Pipe::open().map(Pipe::into_ends)
    }

    fn open_nonblocking_wake_ends() -> io::Result<(fd::OwnedFd, fd::OwnedFd)> {
        use crate::backend::uring::ffi::pipe::Pipe;

        Pipe::open_nonblocking().map(Pipe::into_ends)
    }
}

impl platform::EntropySource for Uring {
    fn acquire() -> io::Result<[u64; 2]> {
        ffi::Entropy::acquire().map(ffi::Entropy::into_words)
    }
}

impl fixed::Lifecycle for Uring {
    fn alloc_slots<'d>(
        &mut self,
        len: u32,
        driver: driver::Reference<'d>,
    ) -> io::Result<fixed::Reservation<'d>> {
        self.fixed_slots.alloc(len, driver)
    }

    fn release_slots<'d>(&mut self, slots: fixed::Reservation<'d>) {
        self.fixed_slots.release(slots);
    }

    fn close<'d>(
        &mut self,
        close: driver::Close<'d>,
        _driver: driver::Reference<'d>,
        phase: fixed::Phase,
    ) {
        match phase {
            fixed::Phase::Active => {
                let Self {
                    ring, lifecycle, ..
                } = self;
                lifecycle.close(close, |work| {
                    submit::Writer::new(&mut *ring).try_close(work)
                });
            }
            fixed::Phase::Final => self.lifecycle.stage_close(close),
        }
    }

    fn retire<'d>(&mut self, slot: fixed::Slot<'d>, phase: fixed::Phase) {
        match phase {
            fixed::Phase::Active => {
                if self.ring.remove_file(slot.fixed()).is_err() {
                    process::abort();
                }
                self.fixed_slots.release_slot(slot);
            }
            fixed::Phase::Final => self.lifecycle.stage_retire(slot),
        }
    }
}
