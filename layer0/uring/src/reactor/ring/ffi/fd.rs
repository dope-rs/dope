use std::{
    marker, mem, num,
    os::fd::{AsRawFd as _, OwnedFd, RawFd},
};

use dope_sys::{io::fd, kernel, ring::register::Unregister};
use libc::c_int;
use o3::base::num::bounded;

use crate::{
    Uring, error,
    reactor::{register, ring, sq},
};

#[derive(Debug)]
pub(crate) struct Fd(OwnedFd);

/// One live io_uring enter authority with its required ABI flag coupled.
#[derive(Clone, Copy)]
pub(crate) struct Binding<'a> {
    raw: u32,
    base: u8,
    owner: marker::PhantomData<&'a ()>,
}

impl Fd {
    pub(crate) fn target(&self) -> Binding<'_> {
        Binding {
            raw: self.0.as_raw_fd() as u32,
            base: 0,
            owner: marker::PhantomData,
        }
    }

    pub(crate) fn raw(&self) -> u32 {
        self.0.as_raw_fd() as u32
    }

    pub(crate) fn setup(
        entries: num::NonZero<u32>,
        params: &mut ring::ffi::Parameters,
    ) -> Result<Self, kernel::Errno<error::Setup>> {
        // SAFETY: params names initialized writable storage of the exact
        // io_uring_params ABI type for the complete syscall.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_uring_setup,
                entries.get() as libc::c_long,
                params as *mut _ as usize as libc::c_long,
            )
        };
        // SAFETY: io_uring_setup returns -1 or transfers one live c_int descriptor;
        // failure reads its unchanged errno before any other native call.
        unsafe {
            <OwnedFd as fd::raw::Adopt>::try_adopt(result as RawFd, || {
                kernel::raw::Code::last::<Uring>().into()
            })
        }
        .map(Self)
    }

    pub(crate) fn register<P, const VALUE: c_int, const COUNT: u32, const OPCODE: u32>(
        &self,
        op: ring::ffi::Register<P, ring::ffi::Exact<VALUE>, COUNT, OPCODE>,
        payload: &mut P,
    ) -> Result<(), kernel::Errno<error::Register<OPCODE>>> {
        self.try_register(op, payload, |status| {
            // SAFETY: op admits exactly VALUE or -1, retaining the syscall's errno.
            unsafe {
                status
                    .admit_exact::<VALUE>()
                    .try_admit(|| kernel::raw::Code::last::<Uring>().into())
            }
        })
    }

    fn try_register<P, O, T, const COUNT: u32, const OPCODE: u32>(
        &self,
        op: ring::ffi::Register<P, O, COUNT, OPCODE>,
        payload: &mut P,
        decode: impl Fn(
            kernel::raw::Status<-1, { i32::MAX }>,
        ) -> Result<T, kernel::Errno<error::Register<OPCODE>>>,
    ) -> Result<T, kernel::Errno<error::Register<OPCODE>>> {
        let opcode = op.opcode();
        loop {
            // SAFETY: op owns P's exact ABI and the exclusive borrow keeps
            // its storage live for the complete syscall.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_io_uring_register,
                    self.0.as_raw_fd() as libc::c_long,
                    opcode as libc::c_long,
                    payload as *mut P as usize as libc::c_long,
                    COUNT as libc::c_long,
                )
            };
            // SAFETY: these io_uring_register operations return -1 or a c_int count.
            let status = unsafe { Uring::status(result) };
            match decode(status) {
                Ok(value) => return Ok(value),
                Err(errno) if errno.is_code(dope_sys::Errno::INTERRUPTED) => continue,
                Err(errno) => return Err(errno),
            }
        }
    }

    pub(crate) fn try_release(
        &self,
        auth: &register::Authority,
    ) -> Result<(), kernel::Errno<Unregister>> {
        let mut payload = ring::ffi::Resource {
            offset: auth.index(),
            reserved: 0,
            data: 0,
        };
        self.register(ring::ffi::UNREGISTER, &mut payload)
            .map_err(|error| kernel::Errno::from(dope_sys::Errno::from(error)))
    }

    pub(crate) fn cancel_all(
        &self,
        payload: &mut ring::ffi::SyncCancel,
    ) -> Result<(), kernel::Errno<error::Register<24>>> {
        match self.cancel(payload) {
            Ok(_) => Ok(()),
            Err(error) if error.is_code(dope_sys::Errno::NOT_FOUND) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// The single synchronous-cancel boundary; an absent target is a result the
    /// callers interpret, not an error this boundary reports.
    pub(crate) fn cancel(
        &self,
        payload: &mut ring::ffi::SyncCancel,
    ) -> Result<bounded::U32<0, { i32::MAX as u32 }>, kernel::Errno<error::Register<24>>> {
        self.try_register(ring::ffi::REGISTER_SYNC_CANCEL, payload, |status| {
            // SAFETY: synchronous cancel returns a c_int count or -1 with unchanged errno.
            let count = unsafe {
                status.try_count::<kernel::Errno<error::Register<24>>>(|| {
                    kernel::raw::Code::last::<Uring>().into()
                })
            }?;
            Ok(count.bounded())
        })
    }

    pub(crate) fn try_enter(
        binding: Binding<'_>,
        submit: sq::Pending,
        complete: u32,
        flags: u32,
        args: *const libc::c_void,
        size: usize,
    ) -> Result<(), kernel::Errno<error::Enter>> {
        let submit: bounded::U32<0, { u32::MAX }> = submit.into();
        let submit = submit.get();
        let Binding {
            raw: target,
            base,
            owner: marker::PhantomData,
        } = binding;
        let flags = flags | u32::from(base);
        // SAFETY: target identifies this ring or its registered index and args
        // names size readable bytes for io_uring_enter.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                target as libc::c_long,
                submit as libc::c_long,
                complete as libc::c_long,
                flags as libc::c_long,
                args as usize as libc::c_long,
                size as libc::c_long,
            )
        };
        // SAFETY: io_uring_enter consumes at most the `submit` submissions it was given.
        unsafe {
            Uring::status(result).try_count::<kernel::Errno<error::Enter>>(|| {
                kernel::raw::Code::last::<Uring>().into()
            })
        }?;
        Ok(())
    }
}

impl<'a> Binding<'a> {
    pub(crate) const fn registered(auth: &'a register::Authority) -> Self {
        Self {
            raw: auth.index(),
            base: ring::ffi::ENTER_REGISTERED as u8,
            owner: marker::PhantomData,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Fd>() == mem::size_of::<RawFd>());
    assert!(mem::size_of::<Binding<'static>>() == 2 * mem::size_of::<u32>());
};
