use std::{mem, num, ptr};

use dope_sys::{io::Close, kernel, memory::Error};
use o3::{base::marker::ThreadBound, mem::Rejected};

use crate::Uring;

/// Thread-affine virtual storage whose writable pages were populated by the kernel.
#[derive(Debug)]
pub struct Mapping<T> {
    ptr: ptr::NonNull<T>,
    bytes: num::NonZero<usize>,
    thread: ThreadBound,
}

type Cleanup = Rejected<Region, Error>;

#[derive(Debug)]
pub struct Region {
    ptr: *mut libc::c_void,
    bytes: num::NonZero<usize>,
    thread: ThreadBound,
}

#[derive(Debug)]
pub(crate) enum Open {
    Failure(Error),
    Cleanup { primary: Error, current: Cleanup },
}

impl Open {
    fn close(primary: Error, region: Region) -> Self {
        match region.try_close() {
            Ok(()) => Self::Failure(primary),
            Err(current) => Self::Cleanup { primary, current },
        }
    }
}

impl<T> Mapping<T> {
    pub(crate) fn allocate(len: usize) -> Result<Self, Open> {
        use kernel::raw::Status;

        let byte_len = Uring::layout::<T>(len).map_err(Open::Failure)?;
        let raw = Uring::try_map::<{ libc::MAP_NORESERVE }>(byte_len).map_err(|cause| {
            Open::Failure(Error::Map {
                bytes: byte_len,
                cause,
            })
        })?;
        let Some(ptr) = ptr::NonNull::new(raw.cast()) else {
            return Err(Open::close(
                Error::Null { bytes: byte_len },
                Region {
                    ptr: raw,
                    bytes: byte_len,
                    thread: ThreadBound::NEW,
                },
            ));
        };
        let mapping = Self {
            ptr,
            bytes: byte_len,
            thread: ThreadBound::NEW,
        };
        // SAFETY: this constructor exclusively owns the complete writable mapping.
        let result = unsafe {
            libc::madvise(
                mapping.ptr.as_ptr().cast(),
                mapping.bytes.get(),
                libc::MADV_POPULATE_WRITE,
            )
        };
        // SAFETY: madvise returns exactly zero or -1 and sets errno on failure.
        match unsafe { Status::new(result).try_zero::<Uring, dope_sys::Errno>() } {
            Ok(()) => Ok(mapping),
            Err(cause) => Err(Open::close(
                Error::Populate {
                    bytes: mapping.bytes,
                    cause,
                },
                mapping.into_region(),
            )),
        }
    }

    pub(crate) fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    pub(crate) fn as_non_null(&self) -> ptr::NonNull<T> {
        self.ptr
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    fn into_region(self) -> Region {
        let Self { ptr, bytes, thread } = self;
        Region {
            ptr: ptr.as_ptr().cast(),
            bytes,
            thread,
        }
    }
}

impl Close for Region {
    type State = Cleanup;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { ptr, bytes, thread } = self;
        // SAFETY: this owner retains the complete successful mapping.
        match unsafe { Uring::try_unmap(ptr, bytes) } {
            Ok(()) => Ok(()),
            Err(cause) => Err(Rejected::new(
                Self { ptr, bytes, thread },
                Error::Unmap {
                    address: ptr,
                    bytes,
                    cause,
                },
            )),
        }
    }
}

impl<T: Copy> Mapping<T> {
    pub(crate) fn new_with(
        len: num::NonZero<u16>,
        mut init: impl FnMut() -> T,
    ) -> Result<Self, Open> {
        let mapping = Self::allocate(usize::from(len.get()))?;
        for index in 0..len.get() {
            // SAFETY: population admitted len properly aligned T slots, each index
            // is visited exactly once, and Copy excludes destructors on failure.
            unsafe { mapping.ptr.as_ptr().add(usize::from(index)).write(init()) };
        }
        Ok(mapping)
    }
}

impl<T> Close for Mapping<T> {
    type State = Cleanup;

    fn try_close(self) -> Result<(), Self::State> {
        self.into_region().try_close()
    }
}

const _: () = {
    assert!(mem::size_of::<Mapping<u8>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Cleanup>() >= mem::size_of::<Region>());
};
