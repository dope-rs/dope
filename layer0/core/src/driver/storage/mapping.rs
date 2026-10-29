use std::mem;

use dope_sys::ring::layout;

use crate::driver::{
    result,
    result::IoOperation,
    storage::{Address, Bytes, Count},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mapping {
    Unregister {
        cause: crate::Errno,
    },
    Length {
        element: Bytes,
        count: Count,
    },
    Alignment {
        element: Bytes,
        supported: Bytes,
    },
    Syscall {
        operation: IoOperation,
        bytes: Bytes,
        cause: result::Kernel,
    },
    Ring {
        operation: IoOperation,
        region: layout::Region,
        bytes: Bytes,
        cause: result::Kernel,
    },
    Unmap {
        operation: IoOperation,
        address: Address,
        bytes: Bytes,
        cause: result::Kernel,
    },
    Null {
        operation: IoOperation,
        bytes: Bytes,
    },
    RingNull {
        operation: IoOperation,
        region: layout::Region,
        bytes: Bytes,
    },
}

impl Mapping {
    pub(crate) const fn unmap(
        op: IoOperation,
        address: Address,
        bytes: Bytes,
        cause: result::Kernel,
    ) -> Self {
        Self::Unmap {
            operation: op,
            address,
            bytes,
            cause,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Address>() == mem::size_of::<*mut core::ffi::c_void>());
    assert!(mem::size_of::<Bytes>() == mem::size_of::<usize>());
};
