use std::{mem, os::fd};

use dope_core::driver::{life::creation, settings};

use crate::{
    executor::{self, create},
    process::{gate, lifecycle, runtime::bus},
};

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

    pub fn control(&self) -> lifecycle::Control<'_> {
        lifecycle::Control::new(self.source.as_fd())
    }

    pub fn terminal(&self) -> crate::process::Terminal<'run> {
        crate::process::Terminal {
            source: self.source,
        }
    }

    pub(super) fn attach<'a>(
        self,
        executor: Result<executor::Executor, create::Error<'a>>,
    ) -> Result<executor::Executor<(), Self>, create::Error<'a>> {
        let terminal = self.terminal();
        executor
            .and_then(|executor| executor.shutdown(self).map_err(create::Error::from))
            .inspect_err(|_| {
                let _ = terminal.fail();
            })
    }
}

impl executor::Build for Shutdown<'_> {
    type Shutdown = Self;

    fn try_build(
        self,
        attempt: &mut creation::Attempt,
        config: settings::Config,
    ) -> Result<executor::Executor<(), Self::Shutdown>, create::Error<'_>> {
        self.attach(executor::Executor::new(attempt, config))
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

    fn failed(&self) {
        let _ = self.terminal().fail();
    }
}

const _: () = {
    assert!(mem::size_of::<Shutdown<'static>>() == mem::size_of::<bus::Tx<'static>>());
    assert!(mem::align_of::<Shutdown<'static>>() == mem::align_of::<bus::Tx<'static>>());
};
