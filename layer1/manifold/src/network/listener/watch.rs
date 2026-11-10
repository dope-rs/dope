use std::{cell::Cell, ops::ControlFlow, task::Poll};

use dope_core::driver::schedule::ready::wake;
use dope_runtime::{
    executor::{self, drive, root, session},
    shutdown,
    topology::Failure,
};

use super::AcceptError;

pub struct Watch<'d> {
    error: Cell<Option<AcceptError>>,
    wake: wake::store::Slot<'d>,
}

#[must_use]
pub struct Wait<'a, 'd>(&'a Watch<'d>);

#[derive(Debug)]
pub enum ExecutionError {
    Accept {
        cause: AcceptError,
        drive: Option<drive::Error>,
    },
    Drive(drive::Error),
}

impl<'d> Watch<'d> {
    pub(super) const fn new() -> Self {
        Self {
            error: Cell::new(None),
            wake: wake::store::Slot::empty(),
        }
    }

    pub(super) fn record(&self, cause: AcceptError) {
        if self.error.get().is_none() {
            self.error.set(Some(cause));
            self.wake.wake_once();
        }
    }

    pub fn error(&self) -> Option<AcceptError> {
        self.error.get()
    }

    /// Resolves the first listener failure after session retirement.
    pub fn finish<'report, I, T>(
        &self,
        result: session::Result<'report, Result<T, ExecutionError>>,
    ) -> Result<T, Failure<'report, I, T, ExecutionError>> {
        match result {
            Ok(output) => self.output(output).map_err(Failure::Execute),
            Err(error) => Err(Failure::Run(match error {
                session::Error::Drive(error) => session::Error::Drive(error),
                session::Error::Install { cause, disarm } => {
                    session::Error::Install { cause, disarm }
                }
                session::Error::Quiesce {
                    output,
                    drive,
                    disarm,
                } => session::Error::Quiesce {
                    output: self.output(output),
                    drive,
                    disarm,
                },
            })),
        }
    }

    fn output<T>(&self, output: Result<T, ExecutionError>) -> Result<T, ExecutionError> {
        let Some(cause) = self.error() else {
            return output;
        };
        let drive = match output {
            Ok(_) => None,
            Err(ExecutionError::Drive(drive)) => Some(drive),
            Err(ExecutionError::Accept { drive, .. }) => drive,
        };
        Err(ExecutionError::Accept { cause, drive })
    }
}

impl<'a, 'd> Wait<'a, 'd> {
    pub(super) const fn new(watch: &'a Watch<'d>) -> Self {
        Self(watch)
    }

    pub const fn watch(&self) -> &'a Watch<'d> {
        self.0
    }

    pub fn poll(&mut self, wake: wake::Waker<'d>) -> Poll<AcceptError> {
        match self.0.error() {
            Some(cause) => Poll::Ready(cause),
            None => {
                self.0.wake.set(wake);
                Poll::Pending
            }
        }
    }

    pub fn run<D, Q>(
        self,
        app: session::App<'_, '_, 'd, D, Q>,
    ) -> Result<shutdown::Requested<Q>, ExecutionError>
    where
        D: executor::raw::App<'d>,
        Q: executor::Startup,
    {
        match app.drive_until(self) {
            Ok(ControlFlow::Break(cause)) => Err(ExecutionError::Accept { cause, drive: None }),
            Ok(ControlFlow::Continue(requested)) => Ok(requested),
            Err(error) => Err(ExecutionError::Drive(error)),
        }
    }
}

impl<'d> root::Root<'d> for Wait<'_, 'd> {
    type Output = AcceptError;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (root, target, _, _) = context.into_parts();
        root.get_mut().poll(wake::Waker::from(target))
    }
}

impl Drop for Wait<'_, '_> {
    fn drop(&mut self) {
        self.0.wake.clear();
    }
}
