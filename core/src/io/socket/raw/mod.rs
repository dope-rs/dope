use std::{cell, io, marker, net, os::fd, pin};

use crate::io::socket;

mod addr;
mod inet;

pub(crate) use addr::Addr;
pub use inet::Inet;

#[repr(transparent)]
pub struct AcceptAddr {
    addr: cell::UnsafeCell<Addr>,
    _pin: marker::PhantomPinned,
}

const _: () = assert!(size_of::<AcceptAddr>() == size_of::<Addr>());
const _: () = assert!(align_of::<AcceptAddr>() == align_of::<Addr>());

impl AcceptAddr {
    pub fn empty() -> Self {
        Self {
            addr: cell::UnsafeCell::new(Addr::empty()),
            _pin: marker::PhantomPinned,
        }
    }

    /// # Safety
    /// No accepted operation may retain this output.
    pub unsafe fn reset(self: pin::Pin<&mut Self>) {
        unsafe { *self.addr.get() = Addr::empty() };
    }

    /// # Safety
    /// The operation that writes this output must be terminal or quiesced.
    pub unsafe fn snapshot(self: pin::Pin<&Self>) -> socket::Addr {
        socket::Addr::from_raw(unsafe { *self.addr.get() })
    }

    /// # Safety
    /// The pinned owner must retain this output through completion or quiescence.
    pub(crate) unsafe fn as_addr_mut(self: pin::Pin<&mut Self>) -> &mut Addr {
        unsafe { &mut *self.addr.get() }
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct Handle {
    fd: fd::OwnedFd,
}

const _: () = assert!(size_of::<Handle>() == size_of::<fd::OwnedFd>());
const _: () = assert!(align_of::<Handle>() == align_of::<fd::OwnedFd>());

impl Handle {
    pub(crate) fn from_owned(fd: fd::OwnedFd) -> Self {
        Self { fd }
    }

    pub(crate) const fn owned(&self) -> &fd::OwnedFd {
        &self.fd
    }

    fn check(rc: libc::c_int) -> io::Result<()> {
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn bind(&self, addr: &socket::Addr) -> io::Result<()> {
        // SAFETY: addr.ptr()/addr.socklen() describe a sockaddr that addr
        // keeps alive for the duration of the call.
        let rc = unsafe {
            use libc::bind;
            bind(
                fd::AsRawFd::as_raw_fd(&self.fd),
                addr.raw().ptr(),
                addr.raw().socklen(),
            )
        };
        Self::check(rc)
    }

    pub(crate) fn listen(&self, backlog: i32) -> io::Result<()> {
        // SAFETY: plain listen on our owned fd; no pointer arguments.
        let rc = unsafe {
            use libc::listen;
            listen(fd::AsRawFd::as_raw_fd(&self.fd), backlog)
        };
        Self::check(rc)
    }

    pub(crate) fn setsockopt_raw(
        &self,
        level: libc::c_int,
        opt: libc::c_int,
        value: libc::c_int,
    ) -> io::Result<()> {
        // SAFETY: the option value is a live local for the duration of the call.
        let rc = unsafe {
            use libc::{c_void, setsockopt, socklen_t};
            setsockopt(
                fd::AsRawFd::as_raw_fd(&self.fd),
                level,
                opt,
                &value as *const libc::c_int as *const c_void,
                size_of::<libc::c_int>() as socklen_t,
            )
        };
        Self::check(rc)
    }

    pub(crate) fn apply_reuse(&self, config: &socket::ListenerConfig) -> io::Result<()> {
        use libc::SOL_SOCKET;
        if config.reuse_addr {
            use libc::SO_REUSEADDR;
            self.setsockopt_raw(SOL_SOCKET, SO_REUSEADDR, 1)?;
        }
        if config.reuse_port {
            use libc::SO_REUSEPORT;
            self.setsockopt_raw(SOL_SOCKET, SO_REUSEPORT, 1)?;
        }
        Ok(())
    }

    pub(crate) fn local_addr(&self) -> io::Result<net::SocketAddr> {
        Addr::from_getsockname(fd::AsRawFd::as_raw_fd(&self.fd))?.into_std()
    }
}

impl From<Handle> for fd::OwnedFd {
    fn from(handle: Handle) -> Self {
        handle.fd
    }
}
