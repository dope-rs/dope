use std::io::{self, Error};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

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
    pub(crate) fn take(fd: RawFd) -> Self {
        assert!(
            fd >= 0,
            "dope invariant violated: io result cannot be negative"
        );
        Self {
            // SAFETY: fd is a fresh descriptor the kernel just handed over,
            // asserted non-negative; ownership transfers to this Handle.
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        }
    }

    pub(crate) fn into_owned(self) -> OwnedFd {
        self.fd
    }

    fn check(rc: libc::c_int) -> io::Result<()> {
        if rc < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn set_cloexec(&self) -> io::Result<()> {
        // SAFETY: plain fcntl on our owned fd; no pointer arguments.
        let rc = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        Self::check(rc)
    }

    pub(crate) fn set_nonblocking(&self) -> io::Result<()> {
        // SAFETY: plain fcntl on our owned fd; no pointer arguments.
        let flags = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_GETFL, 0) };
        if flags < 0 {
            return Err(Error::last_os_error());
        }
        // SAFETY: plain fcntl on our owned fd; no pointer arguments.
        let rc =
            unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
        Self::check(rc)
    }

    pub(crate) fn open(domain: Domain, kind: Kind) -> io::Result<Self> {
        // SAFETY: socket() takes no pointer arguments.
        let raw = unsafe { libc::socket(domain.raw(), kind.raw(), 0) };
        if raw < 0 {
            return Err(Error::last_os_error());
        }
        let sock = Self::take(raw);
        sock.set_cloexec()?;
        Driver::set_no_sigpipe(&sock)?;
        Ok(sock)
    }

    pub(crate) fn bind(&self, addr: &Addr) -> io::Result<()> {
        // SAFETY: addr.ptr()/addr.socklen() describe a sockaddr that addr
        // keeps alive for the duration of the call.
        let rc = unsafe { libc::bind(self.as_raw_fd(), addr.ptr(), addr.socklen()) };
        Self::check(rc)
    }

    pub(crate) fn listen(&self, backlog: i32) -> io::Result<()> {
        // SAFETY: plain listen on our owned fd; no pointer arguments.
        let rc = unsafe { libc::listen(self.as_raw_fd(), backlog) };
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
            libc::setsockopt(
                self.as_raw_fd(),
                level,
                opt,
                &value as *const libc::c_int as *const libc::c_void,
                size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        Self::check(rc)
    }

    pub(crate) fn apply_reuse(&self, config: &ListenerConfig) -> io::Result<()> {
        if config.reuse_addr {
            self.setsockopt_raw(libc::SOL_SOCKET, libc::SO_REUSEADDR, 1)?;
        }
        if config.reuse_port {
            self.setsockopt_raw(libc::SOL_SOCKET, libc::SO_REUSEPORT, 1)?;
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

impl IntoRawFd for Handle {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}
