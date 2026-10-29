use std::{marker, mem, num};

pub use returned::Returned;
pub use source::Source;

pub mod errno;
pub mod raw;

mod returned;
mod source;

/// Exact value observed in the thread-local errno slot after a rejected syscall.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Errno<O>(crate::Errno, marker::PhantomData<fn() -> O>);

impl<O> Errno<O> {
    pub const fn code(&self) -> num::NonZero<i32> {
        self.0.code()
    }

    pub const fn is_code(self, code: crate::Errno) -> bool {
        self.0.code().get() == code.code().get()
    }
}

impl<O> From<crate::Errno> for Errno<O> {
    fn from(code: crate::Errno) -> Self {
        Self(code, marker::PhantomData)
    }
}

impl<O> From<Errno<O>> for crate::Errno {
    fn from(error: Errno<O>) -> Self {
        error.0
    }
}

/// The exact scalar rejected by one native result domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Invalid {
    pub actual: libc::ssize_t,
    pub minimum: libc::ssize_t,
    pub maximum: libc::ssize_t,
}

const _: () = {
    assert!(mem::size_of::<Errno<()>>() == mem::size_of::<i32>());
    assert!(mem::size_of::<Result<(), Errno<()>>>() == mem::size_of::<i32>());
};

/// A nonnegative result admitted at the syscall boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Value(libc::c_int);

impl Value {
    pub const fn get(self) -> libc::c_int {
        self.0
    }
}

/// Canonical kernel-operation and descriptor-table failures for every backend.
/// Diagnostics retain the backend's exact failure without a second error domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Diagnostic<S> {
    Setup(crate::Errno),
    Enter(crate::Errno),
    FileUpdate(crate::Errno),
    Probe(crate::Errno),
    FileTable(crate::Errno),
    RingRegister(crate::Errno),
    BufferRegister(crate::Errno),
    BufferUnregister(crate::Errno),
    Cancel(crate::Errno),
    FileRange(crate::Errno),
    Map(crate::Errno),
    Unmap(crate::Errno),
    Fault(crate::Errno),
    FileIndex(S),
    FileOccupied(S),
}

impl<S> From<crate::Errno> for Diagnostic<S> {
    fn from(cause: crate::Errno) -> Self {
        Self::Fault(cause)
    }
}

/// Native failures that prevent releasing worker or driver storage, or an armed watchdog.
#[derive(Debug)]
pub enum Fatal {
    Join(crate::Errno),
    TimerDelete(crate::Errno),
    /// Native retirement failed while borrowed driver storage remains live.
    Retirement,
}

impl Fatal {
    /// Stops before workers, kernel borrows, or live watchdogs can outlive their owners.
    #[cold]
    pub fn terminate(self) -> ! {
        unreachable!("native")
    }
}
