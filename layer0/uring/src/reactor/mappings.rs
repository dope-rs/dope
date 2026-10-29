use std::{mem, num, ptr};

use dope_sys::{io::Close, kernel, memory::Error};
use o3::{base::marker::ThreadBound, mem::Rejected};

use crate::Uring;

/// Thread-affine virtual storage reserved from the process address space.
#[derive(Debug)]
pub struct Mapping<T> {
    ptr: ptr::NonNull<T>,
    bytes: num::NonZero<usize>,
    thread: ThreadBound,
}

type Unmap<T> = Rejected<Mapping<T>, Error>;
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

/// Mapping whose writable pages were admitted and populated by the kernel.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Populated<T>(Mapping<T>);

impl<T> Mapping<T> {
    pub(crate) fn allocate(len: usize) -> Result<Self, Open> {
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
        Ok(Self {
            ptr,
            bytes: byte_len,
            thread: ThreadBound::NEW,
        })
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

    pub(crate) fn populate(self) -> Result<Populated<T>, Open> {
        use kernel::raw::Status;

        let result = unsafe {
            use libc::MADV_POPULATE_WRITE;
            libc::madvise(
                self.ptr.as_ptr().cast(),
                self.bytes.get(),
                MADV_POPULATE_WRITE,
            )
        };
        // SAFETY: try_zero calls this only when madvise returned -1 on this thread.
        let errno = || unsafe { kernel::raw::Code::last::<Uring>().into() };
        // SAFETY: madvise returns exactly zero or -1 and sets errno on failure.
        match unsafe { Status::new(result).try_zero::<dope_sys::Errno>(errno) } {
            Ok(()) => Ok(Populated(self)),
            Err(cause) => {
                let Self { ptr, bytes, thread } = self;
                Err(Open::close(
                    Error::Populate { bytes, cause },
                    Region {
                        ptr: ptr.as_ptr().cast(),
                        bytes,
                        thread,
                    },
                ))
            }
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
        let Populated(mapping) = Self::allocate(usize::from(len.get()))?.populate()?;
        for index in 0..len.get() {
            // SAFETY: population admitted len properly aligned T slots, each index
            // is visited exactly once, and Copy excludes destructors on failure.
            unsafe { mapping.ptr.as_ptr().add(usize::from(index)).write(init()) };
        }
        Ok(mapping)
    }
}

impl<T> Populated<T> {
    pub(crate) fn as_ptr(&self) -> *const T {
        self.0.as_ptr()
    }

    pub(crate) fn try_close(self) -> Result<(), Unmap<T>> {
        self.0.try_close()
    }
}

impl<T> Close for Mapping<T> {
    type State = Unmap<T>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { ptr, bytes, thread } = self;
        let address = ptr.as_ptr().cast();
        // SAFETY: this owner retains the complete live mapping.
        match unsafe { Uring::try_unmap(address, bytes) } {
            Ok(()) => Ok(()),
            Err(cause) => Err(Rejected::new(
                Self { ptr, bytes, thread },
                Error::Unmap {
                    address,
                    bytes,
                    cause,
                },
            )),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Mapping<u8>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Populated<u8>>() == mem::size_of::<Mapping<u8>>());
    assert!(mem::align_of::<Populated<u8>>() == mem::align_of::<Mapping<u8>>());
    assert!(mem::size_of::<Unmap<u8>>() >= mem::size_of::<Mapping<u8>>());
};
