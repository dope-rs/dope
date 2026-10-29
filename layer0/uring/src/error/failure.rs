use std::mem;

use dope_sys::{Errno, kernel};

use crate::error;

/// Native operation failure backed by [`kernel::Diagnostic`].
/// This local carrier supplies the typed `kernel::Errno` conversions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Failure<S>(kernel::Diagnostic<S>);

impl<S> From<Failure<S>> for kernel::Diagnostic<S> {
    fn from(cause: Failure<S>) -> Self {
        cause.0
    }
}

impl<S> From<Errno> for Failure<S> {
    fn from(cause: Errno) -> Self {
        Self(cause.into())
    }
}

impl<S> From<kernel::Errno<error::Setup>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Setup>) -> Self {
        Self(kernel::Diagnostic::Setup(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Enter>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Enter>) -> Self {
        Self(kernel::Diagnostic::Enter(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<6>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<6>>) -> Self {
        Self(kernel::Diagnostic::FileUpdate(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<8>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<8>>) -> Self {
        Self(kernel::Diagnostic::Probe(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<13>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<13>>) -> Self {
        Self(kernel::Diagnostic::FileTable(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<20>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<20>>) -> Self {
        Self(kernel::Diagnostic::RingRegister(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<22>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<22>>) -> Self {
        Self(kernel::Diagnostic::BufferRegister(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<23>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<23>>) -> Self {
        Self(kernel::Diagnostic::BufferUnregister(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<24>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<24>>) -> Self {
        Self(kernel::Diagnostic::Cancel(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Register<25>>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Register<25>>) -> Self {
        Self(kernel::Diagnostic::FileRange(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Map>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Map>) -> Self {
        Self(kernel::Diagnostic::Map(cause.into()))
    }
}

impl<S> From<kernel::Errno<error::Unmap>> for Failure<S> {
    fn from(cause: kernel::Errno<error::Unmap>) -> Self {
        Self(kernel::Diagnostic::Unmap(cause.into()))
    }
}

const _: () = {
    assert!(mem::size_of::<Failure<u32>>() == mem::size_of::<kernel::Diagnostic<u32>>());
    assert!(mem::align_of::<Failure<u32>>() == mem::align_of::<kernel::Diagnostic<u32>>());
};
