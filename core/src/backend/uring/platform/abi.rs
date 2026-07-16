use std::io::{self, Error, ErrorKind};
use std::mem::size_of;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::{sockaddr_in, sockaddr_in6, sockaddr_un};

use crate::driver::Driver;
use crate::io::ffi::Handle;
use crate::io::socket::Pod;

pub(crate) trait PlatformAbi {
    fn encode_v4(addr: SocketAddrV4) -> libc::sockaddr_in;
    fn encode_v6(addr: SocketAddrV6) -> libc::sockaddr_in6;
    fn encode_unix(path: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)>;
    fn set_no_sigpipe(handle: &Handle) -> io::Result<()>;
    fn open_pipe() -> io::Result<[RawFd; 2]>;
}

impl PlatformAbi for Driver {
    fn encode_v4(addr: SocketAddrV4) -> libc::sockaddr_in {
        let mut sa = sockaddr_in::zeroed();
        sa.sin_family = libc::AF_INET as _;
        sa.sin_port = addr.port().to_be();
        sa.sin_addr = libc::in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        };
        sa
    }

    fn encode_v6(addr: SocketAddrV6) -> libc::sockaddr_in6 {
        let mut sa = sockaddr_in6::zeroed();
        sa.sin6_family = libc::AF_INET6 as _;
        sa.sin6_port = addr.port().to_be();
        sa.sin6_flowinfo = addr.flowinfo();
        sa.sin6_scope_id = addr.scope_id();
        sa.sin6_addr = libc::in6_addr {
            s6_addr: addr.ip().octets(),
        };
        sa
    }

    fn encode_unix(path: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
        let bytes = path.as_os_str().as_bytes();
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "empty path"));
        }

        let mut sa = sockaddr_un::zeroed();
        sa.sun_family = libc::AF_UNIX as _;
        let max = sa.sun_path.len().saturating_sub(1);
        if bytes.len() > max {
            return Err(Error::new(ErrorKind::InvalidInput, "path too long"));
        }
        for (i, byte) in bytes.iter().enumerate() {
            sa.sun_path[i] = *byte as libc::c_char;
        }

        let len = (size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;
        Ok((sa, len))
    }

    fn set_no_sigpipe(_: &Handle) -> io::Result<()> {
        Ok(())
    }

    fn open_pipe() -> io::Result<[RawFd; 2]> {
        let mut fds = [0 as RawFd; 2];
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if rc != 0 {
            return Err(Error::last_os_error());
        }
        Ok(fds)
    }
}
