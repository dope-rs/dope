use std::{mem, os::fd};

use dope_sys::engine::{self, register, shutdown};

use crate::{
    driver,
    driver::{life::native::create, result, storage},
};

/// One unstarted generation with the teardown state its session borrows.
pub struct Domain<Q = ()> {
    live: Option<Generation<Q>>,
    teardown: Teardown,
}

/// Teardown state retained from one started generation until its backend closes.
pub struct Teardown {
    closing: runtime::Closing,
}

/// The unstarted generation: one driver with the single source it retains.
pub struct Generation<Q = ()> {
    driver: driver::Driver,
    source: Source<Q>,
}

/// Exact descriptor receipt retained with its immutably exposed owner.
pub(super) struct Source<Q: ?Sized> {
    descriptor: Option<raw::Descriptor>,
    pub(super) value: Q,
}

impl<Q: ?Sized> Source<Q> {
    pub(in crate::driver::life) fn descriptor(&self) -> Option<fd::BorrowedFd<'_>> {
        self.descriptor.as_ref().map(fd::AsFd::as_fd)
    }
}

mod raw;
pub(crate) mod runtime;
mod session;
pub use session::Session;

impl Domain<()> {
    pub fn new(driver: driver::Driver) -> Self {
        Self {
            live: Some(Generation::new(driver)),
            teardown: Teardown::default(),
        }
    }

    /// Retains the one external source this generation owns, keeping its teardown state.
    pub fn fd<Q: fd::AsFd>(self, source: Q) -> Result<Domain<Q>, Register> {
        let Self { live, teardown } = self;
        let Some(epoch) = live else {
            return Err(Register::Closed);
        };
        Ok(Domain {
            live: Some(epoch.fd(source)?),
            teardown,
        })
    }
}

impl<Q> Domain<Q> {
    /// Acquires the generation once; its exclusive loan retains every source through teardown.
    pub fn start(&mut self) -> Option<Session<'_, Q>> {
        Some(self.live.take()?.start(&mut self.teardown))
    }
}

impl<Q> Generation<Q> {
    /// Starts the generation this value owns; its session borrows this teardown
    /// state until the backend closes.
    pub fn start<'a>(self, teardown: &'a mut Teardown) -> Session<'a, Q> {
        Session {
            driver: self.driver,
            source: self.source,
            teardown,
        }
    }
}

impl Default for Teardown {
    /// Retains no generation: only the state a session borrows through teardown.
    fn default() -> Self {
        Self {
            closing: runtime::Closing::closed(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Register {
    Closed,
    Full(result::SubmitError),
    Register(crate::Errno),
}

impl Register {
    pub(crate) fn native(error: register::Error) -> Self {
        match error {
            register::Error::Full(full) => Self::Full(result::SubmitError::queue(full)),
            register::Error::Register(error) => Self::Register(error),
        }
    }
}

/// Storage and generation-finalization failures preserved without erasure.
pub enum Error<'a, E, T, Q = ()> {
    Storage(E),
    Close(Closing<'a, E, T, Q>),
}

/// Exact preflight failure paired with the operation that has not run.
pub struct Rejected<E, O> {
    pub cause: E,
    pub operation: O,
}

impl<E, O> Rejected<E, O> {
    pub fn map_op<P>(self, map: impl FnOnce(O) -> P) -> Rejected<E, P> {
        Rejected {
            cause: self.cause,
            operation: map(self.operation),
        }
    }
}

impl<'a, E, T, Q> Error<'a, E, T, Q> {
    /// Borrows all failures while retaining the application output and retry ownership.
    pub fn cause(&self) -> Cause<&E> {
        match self {
            Self::Storage(error) => Cause::Storage(error),
            Self::Close(state) => Cause::Close {
                storage: state.result.as_ref().err(),
                failure: state.failure(),
            },
        }
    }

    pub fn map_storage<F>(self, map: impl FnOnce(E) -> F) -> Error<'a, F, T, Q> {
        match self {
            Self::Storage(error) => Error::Storage(map(error)),
            Self::Close(state) => Error::Close(state.map_storage(map)),
        }
    }
}

/// Generation result and source retained until backend teardown completes.
pub struct Closing<'a, E, T, Q> {
    result: Result<T, E>,
    runtime: &'a mut runtime::Closing,
    source: Source<Q>,
    failure: Unmap,
}

#[derive(Clone, Copy, Debug)]
pub enum Unmap {
    Backend(storage::Mapping),
    Context(storage::Mapping),
}

impl Unmap {
    pub(crate) fn native<E>(error: shutdown::Failure<E>) -> Self
    where
        create::Kernel: From<E>,
    {
        match error {
            shutdown::Failure::Backend(error) => Self::Backend(create::Mapping::from(error).0),
            shutdown::Failure::Context(error) => Self::Context(create::Mapping::from(error).0),
        }
    }
}

impl Generation<()> {
    pub fn new(driver: driver::Driver) -> Self {
        Self {
            driver,
            source: Source {
                descriptor: None,
                value: (),
            },
        }
    }

    /// Registers the one external source this generation retains through teardown.
    pub fn fd<Q: fd::AsFd>(self, source: Q) -> Result<Generation<Q>, Register> {
        let Generation {
            mut driver,
            source: Source { value: (), .. },
        } = self;
        let fd = source.as_fd();
        // SAFETY: only shared source loans escape; the descriptor owner stays
        // retained through quiescence and every teardown retry. Native registration
        // fails before publication, so an error retains no descriptor obligation.
        unsafe { engine::raw::Register::register(&mut driver.runtime.backend, fd) }
            .map_err(Register::native)?;
        // SAFETY: Source exposes only shared value loans and drops the receipt
        // before its exact descriptor owner, after backend quiescence or teardown.
        let receipt = unsafe { raw::Descriptor::new(fd) };
        Ok(Generation {
            driver,
            source: Source {
                descriptor: Some(receipt),
                value: source,
            },
        })
    }
}

impl<'a, E, T, Q> Closing<'a, E, T, Q> {
    pub fn map_storage<F>(self, map: impl FnOnce(E) -> F) -> Closing<'a, F, T, Q> {
        Closing {
            result: self.result.map_err(map),
            runtime: self.runtime,
            source: self.source,
            failure: self.failure,
        }
    }

    pub fn result(&self) -> &Result<T, E> {
        &self.result
    }

    pub fn failure(&self) -> Unmap {
        self.failure
    }

    /// Reports the previous failure before retrying the resident owner under its exclusive loan.
    pub fn try_close(self, report: impl FnMut(Unmap)) -> Result<T, Error<'a, E, T, Q>> {
        let Self {
            result,
            runtime,
            source,
            failure: _,
        } = self;
        match runtime.try_close(report) {
            Ok(()) => result.map_err(Error::Storage),
            Err((runtime, failure)) => Err(Error::Close(Self {
                result,
                runtime,
                source,
                failure,
            })),
        }
    }

    fn close(
        result: Result<T, E>,
        runtime: runtime::Runtime,
        teardown: &'a mut Teardown,
        source: Source<Q>,
    ) -> Result<T, Error<'a, E, T, Q>> {
        teardown.closing = runtime.close();
        let runtime = &mut teardown.closing;
        match runtime.failure() {
            Some(failure) => Err(Error::Close(Self {
                result,
                runtime,
                source,
                failure,
            })),
            None => result.map_err(Error::Storage),
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<Session<'static, ()>>()
            == mem::size_of::<driver::Driver>()
                + mem::size_of::<(usize, Option<raw::Descriptor>)>()
    );
    assert!(mem::size_of::<Register>() == mem::size_of::<(u8, crate::Errno)>());
};

/// Every native cause borrowed without taking the stored application output or source.
#[derive(Debug)]
pub enum Cause<E> {
    Storage(E),
    Close { storage: Option<E>, failure: Unmap },
}

impl<E> Cause<E> {
    /// Projects only the storage cause; generation output and retry owners stay borrowed.
    pub fn map_storage<F>(self, map: impl FnOnce(E) -> F) -> Cause<F> {
        match self {
            Self::Storage(error) => Cause::Storage(map(error)),
            Self::Close { storage, failure } => Cause::Close {
                storage: storage.map(map),
                failure,
            },
        }
    }
}
