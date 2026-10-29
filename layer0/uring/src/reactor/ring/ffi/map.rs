use std::{num, ptr};

use dope_sys::ring::layout::Region;

use crate::{
    Mapping, Uring,
    reactor::ring::{Offset, map},
};

#[derive(Debug)]
pub(crate) struct Map {
    address: ptr::NonNull<libc::c_void>,
    len: num::NonZero<usize>,
}

impl Map {
    pub(crate) fn new(region: Region, len: num::NonZero<usize>) -> Result<Self, map::Open> {
        let address = Uring::try_map::<0>(len).map_err(|code| {
            map::Open::Failure(Mapping::Syscall {
                region,
                len,
                code: code.into(),
            })
        })?;
        let Some(address) = ptr::NonNull::new(address) else {
            let failure = Mapping::Null { region, len };
            // SAFETY: mmap returned this live mapping with the exact length.
            return match unsafe { Uring::try_unmap(address, len) } {
                Ok(()) => Err(map::Open::Failure(failure)),
                Err(code) => Err(map::Open::Cleanup {
                    failure,
                    unmap: map::Unmap::new(address, len, code),
                }),
            };
        };
        Ok(Self { address, len })
    }

    pub(crate) fn address(&self) -> u64 {
        const { assert!(usize::BITS <= u64::BITS) };
        self.address.as_ptr() as usize as u64
    }

    pub(crate) fn at<T>(&self, offset: Offset<T>) -> ptr::NonNull<T> {
        unsafe {
            self.address
                .cast::<u8>()
                .add(offset.get() as usize)
                .cast::<T>()
        }
    }

    pub(crate) fn into_region(self) -> map::Region {
        map::Region::new(self.address.as_ptr(), self.len)
    }

    pub(crate) fn release(region: map::Region) -> Result<(), map::Unmap> {
        // SAFETY: Unmap owns this complete live mmap region until success.
        match unsafe { Uring::try_unmap(region.address(), region.len()) } {
            Ok(()) => Ok(()),
            Err(code) => Err(map::Unmap::new(region.address(), region.len(), code)),
        }
    }
}
