use std::convert::Infallible;

use dope_sys::{memory, ring::map};

pub(crate) use crate::driver::storage::Mapping;
use crate::driver::{
    life::native::create::Kernel,
    result::IoOperation,
    storage::{Address, Bytes, Count},
};
impl<R> From<(R, Infallible)> for Mapping {
    fn from((_, empty): (R, Infallible)) -> Self {
        match empty {}
    }
}

impl From<memory::Error> for Mapping {
    fn from(error: memory::Error) -> Self {
        match error {
            memory::Error::Length { element, count } => Self::Length {
                element: Bytes::new(element),
                count: Count::new(count),
            },
            memory::Error::Alignment {
                element,
                supported: limit,
            } => Self::Alignment {
                element: Bytes::new(element),
                supported: Bytes::new(limit),
            },
            memory::Error::Map { bytes, cause } => Self::Syscall {
                operation: IoOperation::MapBuffers,
                bytes: Bytes::new(bytes.get()),
                cause: Kernel::from(cause),
            },
            memory::Error::Populate { bytes, cause } => Self::Syscall {
                operation: IoOperation::PopulateBuffers,
                bytes: Bytes::new(bytes.get()),
                cause: Kernel::from(cause),
            },
            memory::Error::Null { bytes } => Self::Null {
                operation: IoOperation::MapBuffers,
                bytes: Bytes::new(bytes.get()),
            },
            memory::Error::Unmap {
                address,
                bytes,
                cause,
            } => Self::Unmap {
                operation: IoOperation::UnmapBuffers,
                address: Address::new(address),
                bytes: Bytes::new(bytes.get()),
                cause: Kernel::from(cause),
            },
        }
    }
}

impl<E> From<map::Storage<E>> for Mapping
where
    Kernel: From<E>,
{
    fn from(error: map::Storage<E>) -> Self {
        match error {
            map::Storage::Ring(failure) => Self::from(failure),
            map::Storage::Buffer(failure) => Self::from(failure),
            map::Storage::Unregister(cause) => Self::Unregister {
                cause: cause.into(),
            },
        }
    }
}

impl<E> From<map::Map<E>> for Mapping
where
    Kernel: From<E>,
{
    fn from(error: map::Map<E>) -> Self {
        match error {
            map::Map::Syscall { region, len, code } => Self::Ring {
                operation: IoOperation::MapRing,
                region,
                bytes: Bytes::new(len.get()),
                cause: Kernel::from(code),
            },
            map::Map::Null { region, len } => Self::RingNull {
                operation: IoOperation::MapRing,
                region,
                bytes: Bytes::new(len.get()),
            },
        }
    }
}

impl<E> From<map::Failure<E>> for Mapping
where
    Kernel: From<E>,
{
    fn from(failure: map::Failure<E>) -> Self {
        let (address, bytes, code) = failure.into_parts();
        Self::unmap(
            IoOperation::UnmapRing,
            Address::new(address),
            Bytes::new(bytes.get()),
            Kernel::from(code),
        )
    }
}
