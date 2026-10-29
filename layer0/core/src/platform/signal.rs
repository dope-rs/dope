use std::{cell, marker, mem, os::fd};

use dope_sys::thread::{
    Thread,
    signal::{self, AcquireFault, ReleaseFault, Signal},
};
use o3::{base::marker::ThreadBound, mem::slot};

use crate::{Backend, Errno};

pub type Receiver = <Backend as Thread>::Receiver;

#[derive(Debug)]
pub enum Error<'owner, E> {
    Acquire(Acquire),
    Run(E),
    Release {
        run: Option<E>,
        cause: Release<'owner>,
    },
}

thread_local! {
    static CLAIMED: cell::Cell<bool> = const { cell::Cell::new(false) };
}

/// A native signal policy that could not yet be restored on its owning thread.
#[derive(Debug)]
#[must_use = "the signal policy must be restored before another scope can start"]
pub struct Release<'owner> {
    pub fault: ReleaseFault,
    receiver: slot::Occupied<'owner, Receiver>,
}

impl Release<'_> {
    pub fn retry(self) -> Result<(), Self> {
        release(self.receiver)
    }
}

fn release(receiver: slot::Occupied<'_, Receiver>) -> Result<(), Release<'_>> {
    match signal::raw::Receiver::restore(&*receiver) {
        Ok(()) => {
            drop(receiver);
            CLAIMED.with(|claimed| claimed.set(false));
            Ok(())
        }
        Err(fault) => Err(Release { fault, receiver }),
    }
}

type State<'process> = (
    <Receiver as signal::raw::Receiver>::Source<'process>,
    marker::PhantomData<(cell::Cell<&'process ()>, ThreadBound)>,
);

/// Linear process-lifetime view of the termination signal receiver.
#[must_use]
pub struct Source<'process> {
    read: State<'process>,
}

impl Signal for Source<'_> {
    type Error = <Receiver as signal::raw::Receiver>::Wait;

    fn wait(&self) -> Result<(), Self::Error> {
        self.read.0.wait()
    }
}

impl Source<'_> {
    /// Enters the exclusive signal scope on the process's original thread.
    /// Call during single-threaded startup. Other code must not change SIGTERM
    /// handling while `run` executes; masks on other existing threads are not updated.
    /// Workers must start inside `run` and finish before the policy is restored.
    pub fn scope<'owner, R, E>(
        retained: slot::Vacant<'owner, Receiver>,
        run: impl for<'process> FnOnce(Source<'process>) -> Result<R, E>,
    ) -> Result<R, Error<'owner, E>> {
        if !<Backend as Thread>::is_main() {
            return Err(Error::Acquire(Acquire::Thread));
        }
        if CLAIMED.with(|claimed| claimed.replace(true)) {
            return Err(Error::Acquire(Acquire::Busy));
        }
        // SAFETY: this claim excludes every other signal scope until successful
        // restoration. The receiver stays on this thread through release.
        let receiver = match unsafe { <Receiver as signal::raw::Receiver>::try_new() } {
            Ok(receiver) => retained.insert(receiver),
            Err(error) => {
                CLAIMED.with(|claimed| claimed.set(false));
                return Err(Error::Acquire(Acquire::from_native(error.into())));
            }
        };
        let source = Source {
            read: (
                signal::raw::Receiver::source(&*receiver),
                marker::PhantomData,
            ),
        };
        let outcome = run(source);
        match release(receiver) {
            Ok(()) => outcome.map_err(Error::Run),
            Err(cause) => Err(Error::Release {
                run: outcome.err(),
                cause,
            }),
        }
    }
}

impl fd::AsFd for Source<'_> {
    fn as_fd(&self) -> fd::BorrowedFd<'_> {
        self.read.0.as_fd()
    }
}

const _: () = {
    assert!(mem::size_of::<Source<'static>>() == mem::size_of::<fd::BorrowedFd<'static>>());
    assert!(mem::align_of::<Source<'static>>() == mem::align_of::<fd::BorrowedFd<'static>>());
    assert!(
        mem::size_of::<Option<Source<'static>>>()
            == mem::size_of::<Option<fd::BorrowedFd<'static>>>()
    );
};

#[derive(Debug)]
pub enum Acquire {
    Thread,
    Busy,
    MaskBlock(Errno),
    SignalFd(Errno),
    QueueOpen(Errno),
    QueueCloexec(Errno),
    Register(Errno),
    Ignore(Errno),
}

impl Acquire {
    fn from_native(error: AcquireFault) -> Self {
        match error {
            AcquireFault::MaskBlock(error) => Self::MaskBlock(error),
            AcquireFault::SignalFd(error) => Self::SignalFd(error),
            AcquireFault::QueueOpen(error) => Self::QueueOpen(error),
            AcquireFault::QueueCloexec(error) => Self::QueueCloexec(error),
            AcquireFault::Register(error) => Self::Register(error),
            AcquireFault::Ignore(error) => Self::Ignore(error),
        }
    }
}
