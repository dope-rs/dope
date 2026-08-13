use std::os::fd;

use crate::{
    backend::{
        self, fixed,
        kqueue::engine::{event, write},
    },
    driver::{self, flight},
    io,
};

#[repr(transparent)]
pub(crate) struct Control<'a> {
    backend: &'a mut backend::Kqueue,
}

impl<'a> Control<'a> {
    pub(in crate::backend::kqueue) fn new(backend: &'a mut backend::Kqueue) -> Self {
        Self { backend }
    }
}

impl Control<'_> {
    pub(in crate::backend::kqueue) fn reclaim(
        &mut self,
        completion: event::Completion,
        drain: &flight::Drain<'_, '_>,
    ) {
        let driver = drain.driver();
        let completion = completion.into_completion(&mut self.backend.files, drain);
        match completion.into_reclaim(driver) {
            io::Reclaim::Accepted(accepted) => fixed::Lifecycle::close(
                self.backend,
                driver::Close::untracked(accepted.into_slot()),
                driver,
                fixed::Phase::Active,
            ),
            io::Reclaim::Close(close) => {
                fixed::Lifecycle::close(self.backend, close, driver, fixed::Phase::Active)
            }
            io::Reclaim::Slots(retired) => {
                let slots = driver.outbound().take_retired_slots(retired);
                fixed::Lifecycle::release_slots(self.backend, slots);
            }
            io::Reclaim::Buffer(_) | io::Reclaim::None => {}
        }
    }

    pub(crate) fn close_owned(&mut self, fd: fd::OwnedFd) {
        let raw = fd::AsRawFd::as_raw_fd(&fd);
        if let Some(target) = self.backend.reads.remove_fd(raw) {
            self.backend.pending.suppress_target(target);
        }
        if let Some(target) = write::Retry::new(self.backend).cancel_write_retry(raw) {
            self.backend.pending.suppress_target(target);
        }
        self.backend
            .poll
            .changes
            .remove(raw as libc::uintptr_t, libc::EVFILT_READ);
        self.backend
            .poll
            .changes
            .remove(raw as libc::uintptr_t, libc::EVFILT_WRITE);
    }
}
