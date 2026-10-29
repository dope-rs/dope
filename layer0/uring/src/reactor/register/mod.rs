type Invariant<T> = marker::PhantomData<(ThreadBound, fn(T) -> T)>;

mod attempt;
mod cleanup;
mod closing;
pub(crate) use attempt::Attempt;
pub(crate) use cleanup::Cleanup;
pub(crate) use closing::Closing;
use dope_sys::{
    io::reactor::complete,
    kernel,
    ring::{register, register::Occupied as _},
};
mod probe;
pub(crate) mod raw;

use std::{marker, mem, num, os::fd, ptr};

use o3::base::{marker::ThreadBound, num::bounded};
pub(crate) use probe::Probe;

use crate::{
    error,
    reactor::{buffers, ring, sq},
};

#[derive(Debug)]
pub(crate) struct Authority(bounded::U32<0, { u32::MAX - 1 }>);

pub struct Register<'a, T> {
    fd: &'a ring::ffi::Fd,
    local: Invariant<T>,
}

type Remove = register::Remove<raw::Occupied, kernel::Errno<error::Register<6>>>;

impl Authority {
    pub(crate) const fn index(&self) -> u32 {
        self.0.get()
    }
}

impl<'a, T> Register<'a, T> {
    pub(crate) const fn new(fd: &'a ring::ffi::Fd) -> Self {
        Self {
            fd,
            local: marker::PhantomData,
        }
    }

    pub(crate) fn buffer(
        &mut self,
        address: ptr::NonNull<buffers::Entry>,
        entries: num::NonZero<u16>,
        group: u16,
    ) -> Result<(), kernel::Errno<error::Register<22>>> {
        use crate::reactor::ring::ffi;

        let mut payload = ffi::Buffers {
            ring_address: address.as_ptr() as u64,
            ring_entries: u32::from(entries.get()),
            group,
            flags: 0,
            reserved: [0; 3],
        };
        self.fd.register(ffi::REGISTER_BUFFER, &mut payload)?;
        Ok(())
    }

    pub(crate) fn unbuffer(
        &mut self,
        group: u16,
    ) -> Result<(), kernel::Errno<error::Register<23>>> {
        use crate::reactor::ring::ffi;

        let mut payload = ffi::Buffers {
            ring_address: 0,
            ring_entries: 0,
            group,
            flags: 0,
            reserved: [0; 3],
        };
        self.fd.register(ffi::UNREGISTER_BUFFER, &mut payload)?;
        Ok(())
    }

    /// Registers this ring's own descriptor, letting the kernel allocate its fixed-file
    /// index: `IORING_FILE_INDEX_ALLOC` requests allocation, and a successful call writes
    /// the chosen index back, so only a refused call leaves the sentinel.
    pub(crate) fn register(&mut self) -> Result<Authority, kernel::Errno<error::Register<20>>> {
        use crate::reactor::ring::ffi;

        let mut payload = ffi::Resource {
            offset: ffi::FILE_INDEX_ALLOC,
            reserved: 0,
            data: u64::from(self.fd.raw()),
        };
        self.fd.register(ffi::REGISTER, &mut payload)?;
        // SAFETY: the successful registration above replaced the
        // FILE_INDEX_ALLOC sentinel with the kernel-allocated fixed-file index,
        // and a ring's file table indexes from zero, so it never holds u32::MAX.
        let index = unsafe { bounded::raw::Proof::<0, { u32::MAX - 1 }>::assume(payload.offset) };
        Ok(Authority(index.retain()))
    }

    fn prepare(address: u64, flags: u32) -> ring::ffi::SyncCancel {
        use crate::reactor::ring::ffi;

        ffi::SyncCancel {
            address,
            file: -1,
            flags,
            timeout: ffi::Timespec {
                seconds: -1,
                nanoseconds: -1,
            },
            opcode: 0,
            reserved: [0; 7],
            reserved2: [0; 3],
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Authority>() == mem::size_of::<u32>());
    assert!(mem::size_of::<Register<'static, ()>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Register<'static, ()>>() == mem::align_of::<usize>());
};

impl<T> register::Registry for Register<'_, T> {
    type Token = T;
    type Target = sq::Fixed;
    type File = raw::Occupied;
    type Cancel = kernel::Errno<error::Register<24>>;
    type Files = kernel::Errno<error::Register<13>>;
    type Range = kernel::Errno<error::Register<25>>;
    type Update = kernel::Errno<error::Register<6>>;
    fn cancel_all(&mut self) -> Result<(), kernel::Errno<error::Register<24>>> {
        let mut payload = Self::prepare(0, 1 << 2);
        self.fd.cancel_all(&mut payload)
    }
    fn cancel(
        &mut self,
        token: T,
    ) -> Result<bounded::U32<0, { i32::MAX as u32 }>, kernel::Errno<error::Register<24>>>
    where
        T: complete::raw::Token,
    {
        let mut payload = Self::prepare(sq::raw::Word::encode(token).get(), 1);
        self.fd.cancel(&mut payload)
    }
    fn sparse_files<const MAX: u32>(
        &mut self,
        count: bounded::U32<1, MAX>,
    ) -> Result<(), kernel::Errno<error::Register<13>>> {
        use crate::reactor::ring::ffi;

        let mut payload = ffi::Resources {
            count: count.get(),
            flags: ffi::RESOURCE_REGISTER_SPARSE,
            reserved: 0,
            data: 0,
            tags: 0,
        };
        self.fd.register(ffi::REGISTER_FILES, &mut payload)
    }
    fn file_range<const MAX: u32>(
        &mut self,
        len: bounded::U32<1, MAX>,
    ) -> Result<(), kernel::Errno<error::Register<25>>> {
        use crate::reactor::ring::ffi;

        let mut payload = ffi::FileRange {
            offset: 0,
            len: len.get(),
            reserved: 0,
        };
        self.fd.register(ffi::REGISTER_FILE_RANGE, &mut payload)
    }
    fn install(
        &mut self,
        slot: sq::Fixed,
        file: fd::BorrowedFd<'_>,
    ) -> Result<(), kernel::Errno<error::Register<6>>> {
        use fd::AsRawFd as _;

        use crate::reactor::ring::ffi;

        let raw = file.as_raw_fd();
        let mut payload = ffi::FilesUpdate {
            offset: slot.index(),
            reserved: 0,
            files: ptr::from_ref(&raw) as u64,
        };
        self.fd.register(ffi::REGISTER_FILES_UPDATE, &mut payload)
    }
    fn occupy(
        &mut self,
        slot: sq::Fixed,
        file: fd::BorrowedFd<'_>,
    ) -> Result<raw::Occupied, kernel::Errno<error::Register<6>>> {
        self.install(slot, file)?;
        Ok(raw::Occupied::new(slot))
    }
    fn try_remove(&mut self, file: raw::Occupied) -> Result<(), Remove> {
        use crate::reactor::ring::ffi;

        let raw = -1;
        let mut payload = ffi::FilesUpdate {
            offset: file.slot().index(),
            reserved: 0,
            files: ptr::from_ref(&raw) as u64,
        };
        match self.fd.register(ffi::REGISTER_FILES_UPDATE, &mut payload) {
            Ok(()) => Ok(()),
            Err(failure) => Err(Remove { file, failure }),
        }
    }
}
