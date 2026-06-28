use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use super::socket::{Addr, Domain, Kind};

#[derive(Debug)]
pub(super) struct OsFd {
    fd: OwnedFd,
}

impl OsFd {
    #[must_use]
    pub(super) fn take(fd: RawFd) -> Self {
        assert!(
            fd >= 0,
            "dope invariant violated: io result cannot be negative"
        );
        // SAFETY: `fd` is non-negative (asserted above) and the caller transfers ownership.
        Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        }
    }

    fn check(rc: libc::c_int) -> io::Result<()> {
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn set_cloexec(&self) -> io::Result<()> {
        // SAFETY: the fd is owned by `self` and live for this borrow.
        let rc = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        Self::check(rc)
    }

    pub(super) fn set_nonblocking(&self) -> io::Result<()> {
        // SAFETY: the fd is owned by `self` and live for this borrow.
        let flags = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_GETFL, 0) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the fd is owned by `self` and live for this borrow.
        let rc =
            unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
        Self::check(rc)
    }

    pub(super) fn open(domain: Domain, kind: Kind) -> io::Result<Self> {
        // SAFETY: socket(2) with constant family/type args; the return is checked below.
        let raw = unsafe { libc::socket(domain.raw(), kind.raw(), 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let sock = Self::take(raw);
        sock.set_cloexec()?;
        Ok(sock)
    }

    pub(super) fn bind(&self, addr: &Addr) -> io::Result<()> {
        // SAFETY: the fd is owned by `self`; `addr` exposes a sockaddr pointer plus matching length for this call.
        let rc = unsafe { libc::bind(self.as_raw_fd(), addr.ptr(), addr.socklen()) };
        Self::check(rc)
    }

    pub(super) fn listen(&self, backlog: i32) -> io::Result<()> {
        // SAFETY: the fd is owned by `self` and live for this borrow.
        let rc = unsafe { libc::listen(self.as_raw_fd(), backlog) };
        Self::check(rc)
    }

    pub(super) fn setsockopt_raw(
        &self,
        level: libc::c_int,
        opt: libc::c_int,
        value: libc::c_int,
    ) -> io::Result<()> {
        // SAFETY: the fd is owned by `self`; `&value` points to a live c_int whose size is passed as optlen.
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

    pub(super) fn apply_reuse(&self, opts: &super::ListenerOpts) -> io::Result<()> {
        if opts.reuse_addr {
            self.setsockopt_raw(libc::SOL_SOCKET, libc::SO_REUSEADDR, 1)?;
        }
        if opts.reuse_port {
            self.setsockopt_raw(libc::SOL_SOCKET, libc::SO_REUSEPORT, 1)?;
        }
        Ok(())
    }

    pub(super) fn local_addr(&self) -> io::Result<SocketAddr> {
        Addr::from_getsockname(self.as_raw_fd())?.to_std()
    }
}

impl AsRawFd for OsFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl IntoRawFd for OsFd {
    fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }
}
