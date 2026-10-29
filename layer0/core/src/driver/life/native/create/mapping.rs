use std::convert::Infallible;

use dope_sys::{memory, ring::map};

use crate::driver::{
    life::native::create::Kernel,
    result::IoOperation,
    storage,
    storage::{Address, Bytes, Count},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Mapping(pub(crate) storage::Mapping);
impl<R> From<(R, Infallible)> for Mapping {
    fn from((_, empty): (R, Infallible)) -> Self {
        match empty {}
    }
}

impl From<memory::Error> for Mapping {
    fn from(error: memory::Error) -> Self {
        Self(match error {
            memory::Error::Length { element, count } => storage::Mapping::Length {
                element: Bytes::new(element),
                count: Count::new(count),
            },
            memory::Error::Alignment {
                element,
                supported: limit,
            } => storage::Mapping::Alignment {
                element: Bytes::new(element),
                supported: Bytes::new(limit),
            },
            memory::Error::Map { bytes, cause } => storage::Mapping::Syscall {
                operation: IoOperation::MapBuffers,
                bytes: Bytes::new(bytes.get()),
                cause: Kernel::from(cause).0,
            },
            memory::Error::Populate { bytes, cause } => storage::Mapping::Syscall {
                operation: IoOperation::PopulateBuffers,
                bytes: Bytes::new(bytes.get()),
                cause: Kernel::from(cause).0,
            },
            memory::Error::Null { bytes } => storage::Mapping::Null {
                operation: IoOperation::MapBuffers,
                bytes: Bytes::new(bytes.get()),
            },
            memory::Error::Unmap {
                address,
                bytes,
                cause,
            } => storage::Mapping::Unmap {
                operation: IoOperation::UnmapBuffers,
                address: Address::new(address),
                bytes: Bytes::new(bytes.get()),
                cause: Kernel::from(cause).0,
            },
        })
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
            map::Storage::Unregister(cause) => Self(storage::Mapping::Unregister {
                cause: cause.into(),
            }),
        }
    }
}

impl<E> From<map::Map<E>> for Mapping
where
    Kernel: From<E>,
{
    fn from(error: map::Map<E>) -> Self {
        Self(match error {
            map::Map::Syscall { region, len, code } => storage::Mapping::Ring {
                operation: IoOperation::MapRing,
                region,
                bytes: Bytes::new(len.get()),
                cause: Kernel::from(code).0,
            },
            map::Map::Null { region, len } => storage::Mapping::RingNull {
                operation: IoOperation::MapRing,
                region,
                bytes: Bytes::new(len.get()),
            },
        })
    }
}

impl<E> From<map::Failure<E>> for Mapping
where
    Kernel: From<E>,
{
    fn from(failure: map::Failure<E>) -> Self {
        let (address, bytes, code) = failure.into_parts();
        Self(storage::Mapping::unmap(
            IoOperation::UnmapRing,
            Address::new(address),
            Bytes::new(bytes.get()),
            Kernel::from(code).0,
        ))
    }
}
