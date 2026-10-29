use std::{mem, os::fd};

use crate::process::{gate, runtime::bus};

/// Shutdown source borrowed from the exact process run that owns this worker.
///
/// A proof issued by the standalone shutdown API cannot be retagged as a
/// production process proof.
pub struct Shutdown<'run> {
    pub(in crate::process) source: bus::Tx<'run>,
}

impl<'run> Shutdown<'run> {
    pub(super) const fn new(tx: bus::Tx<'run>) -> Self {
        Self { source: tx }
    }

    pub(super) fn gate(&self) -> gate::Closed<'run> {
        gate::Closed::new(self.source)
    }
}

impl fd::AsFd for Shutdown<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.source.as_fd()
    }
}

impl crate::executor::Startup for Shutdown<'_> {
    fn installed(&self) -> Result<(), crate::executor::InstallError> {
        self.source.installed().map_err(Into::into)
    }
}

const _: () = {
    assert!(mem::size_of::<Shutdown<'static>>() == mem::size_of::<bus::Tx<'static>>());
    assert!(mem::align_of::<Shutdown<'static>>() == mem::align_of::<bus::Tx<'static>>());
};
