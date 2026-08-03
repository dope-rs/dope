use std::io;
use std::io::Error;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use libc::{
    F_GETFL, F_SETFD, F_SETFL, FD_CLOEXEC, O_NONBLOCK, SO_REUSEADDR, SO_REUSEPORT, SOL_SOCKET,
    bind, c_int, c_void, fcntl, listen, setsockopt, socket, socklen_t,
};

use crate::driver::Driver;
use crate::io::socket::addr::Addr;
use crate::io::socket::{Domain, Kind, ListenerConfig};
use crate::platform::raw::abi::PlatformAbi;

#[derive(Debug)]
pub(crate) struct Handle {
    fd: OwnedFd,
}

impl Handle {
    #[must_use]
    pub(crate) fn from_owned(fd: OwnedFd) -> Self {
        Self { fd }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn into_owned(self) -> OwnedFd {
        self.fd
    }

    fn check(rc: c_int) -> io::Result<()> {
        if rc < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn set_cloexec(&self) -> io::Result<()> {
        // SAFETY: plain fcntl on our owned fd; no pointer arguments.
        let rc = unsafe { fcntl(self.fd.as_raw_fd(), F_SETFD, FD_CLOEXEC) };
        Self::check(rc)
    }

    pub(crate) fn set_nonblocking(&self) -> io::Result<()> {
        // SAFETY: plain fcntl on our owned fd; no pointer arguments.
        let flags = unsafe { fcntl(self.fd.as_raw_fd(), F_GETFL, 0) };
        if flags < 0 {
            return Err(Error::last_os_error());
        }
        // SAFETY: plain fcntl on our owned fd; no pointer arguments.
        let rc = unsafe { fcntl(self.fd.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) };
        Self::check(rc)
    }

    pub(crate) fn open(domain: Domain, kind: Kind) -> io::Result<Self> {
        // SAFETY: socket() takes no pointer arguments.
        let raw = unsafe { socket(domain.raw(), kind.raw(), 0) };
        if raw < 0 {
            return Err(Error::last_os_error());
        }
        // SAFETY: socket returned a fresh owned descriptor.
        let sock = Self::from_owned(unsafe { OwnedFd::from_raw_fd(raw) });
        sock.set_cloexec()?;
        Driver::set_no_sigpipe(&sock)?;
        Ok(sock)
    }

    pub(crate) fn bind(&self, addr: &Addr) -> io::Result<()> {
        // SAFETY: addr.ptr()/addr.socklen() describe a sockaddr that addr
        // keeps alive for the duration of the call.
        let rc = unsafe { bind(self.as_raw_fd(), addr.ptr(), addr.socklen()) };
        Self::check(rc)
    }

    pub(crate) fn listen(&self, backlog: i32) -> io::Result<()> {
        // SAFETY: plain listen on our owned fd; no pointer arguments.
        let rc = unsafe { listen(self.as_raw_fd(), backlog) };
        Self::check(rc)
    }

    pub(crate) fn setsockopt_raw(&self, level: c_int, opt: c_int, value: c_int) -> io::Result<()> {
        // SAFETY: the option value is a live local for the duration of the call.
        let rc = unsafe {
            setsockopt(
                self.as_raw_fd(),
                level,
                opt,
                &value as *const c_int as *const c_void,
                size_of::<c_int>() as socklen_t,
            )
        };
        Self::check(rc)
    }

    pub(crate) fn apply_reuse(&self, config: &ListenerConfig) -> io::Result<()> {
        if config.reuse_addr {
            self.setsockopt_raw(SOL_SOCKET, SO_REUSEADDR, 1)?;
        }
        if config.reuse_port {
            self.setsockopt_raw(SOL_SOCKET, SO_REUSEPORT, 1)?;
        }
        Ok(())
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        Addr::from_getsockname(self.as_raw_fd())?.into_std()
    }
}

impl AsRawFd for Handle {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
