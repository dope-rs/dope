use std::io::{self, Error, ErrorKind};
use std::mem::size_of;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::{sockaddr_in, sockaddr_in6, sockaddr_un};

use crate::driver::Driver;
use crate::io::ffi::Handle;
use crate::io::socket::Pod;

pub(crate) trait PlatformAbi {
    fn sockaddr_v4() -> sockaddr_in;
    fn sockaddr_v6() -> sockaddr_in6;
    fn sockaddr_un() -> sockaddr_un;
    fn finish_unix(addr: &mut sockaddr_un, len: libc::socklen_t);
    fn set_no_sigpipe(handle: &Handle) -> io::Result<()>;
    fn open_pipe() -> io::Result<[OwnedFd; 2]>;

    fn encode_v4(addr: SocketAddrV4) -> sockaddr_in {
        let mut encoded = Self::sockaddr_v4();
        encoded.sin_family = libc::AF_INET as _;
        encoded.sin_port = addr.port().to_be();
        encoded.sin_addr = libc::in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        };
        encoded
    }

    fn encode_v6(addr: SocketAddrV6) -> sockaddr_in6 {
        let mut encoded = Self::sockaddr_v6();
        encoded.sin6_family = libc::AF_INET6 as _;
        encoded.sin6_port = addr.port().to_be();
        encoded.sin6_flowinfo = addr.flowinfo();
        encoded.sin6_scope_id = addr.scope_id();
        encoded.sin6_addr = libc::in6_addr {
            s6_addr: addr.ip().octets(),
        };
        encoded
    }

    fn encode_unix(path: &Path) -> io::Result<(sockaddr_un, libc::socklen_t)> {
        let bytes = path.as_os_str().as_bytes();
        if bytes.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "empty path"));
        }
        let mut encoded = Self::sockaddr_un();
        encoded.sun_family = libc::AF_UNIX as _;
        let max = encoded.sun_path.len().saturating_sub(1);
        if bytes.len() > max {
            return Err(Error::new(ErrorKind::InvalidInput, "path too long"));
        }
        for (index, byte) in bytes.iter().enumerate() {
            encoded.sun_path[index] = *byte as libc::c_char;
        }
        let len = (size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;
        Self::finish_unix(&mut encoded, len);
        Ok((encoded, len))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{
        Driver, Error, FromRawFd, Handle, OwnedFd, PlatformAbi, Pod, RawFd, io, sockaddr_in,
        sockaddr_in6, sockaddr_un,
    };

    impl PlatformAbi for Driver {
        fn sockaddr_v4() -> sockaddr_in {
            sockaddr_in::zeroed()
        }

        fn sockaddr_v6() -> sockaddr_in6 {
            sockaddr_in6::zeroed()
        }

        fn sockaddr_un() -> sockaddr_un {
            sockaddr_un::zeroed()
        }

        fn finish_unix(_addr: &mut sockaddr_un, _len: libc::socklen_t) {}

        fn set_no_sigpipe(_handle: &Handle) -> io::Result<()> {
            Ok(())
        }

        fn open_pipe() -> io::Result<[OwnedFd; 2]> {
            let mut fds = [0 as RawFd; 2];
            let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
            if rc != 0 {
                return Err(Error::last_os_error());
            }
            let [read, write] = fds;
            let read = unsafe { OwnedFd::from_raw_fd(read) };
            let write = unsafe { OwnedFd::from_raw_fd(write) };
            Ok([read, write])
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use super::{
        Driver, Error, FromRawFd, Handle, OwnedFd, PlatformAbi, Pod, RawFd, io, size_of,
        sockaddr_in, sockaddr_in6, sockaddr_un,
    };

    impl PlatformAbi for Driver {
        fn sockaddr_v4() -> sockaddr_in {
            let mut addr = sockaddr_in::zeroed();
            addr.sin_len = size_of::<sockaddr_in>() as u8;
            addr
        }

        fn sockaddr_v6() -> sockaddr_in6 {
            let mut addr = sockaddr_in6::zeroed();
            addr.sin6_len = size_of::<sockaddr_in6>() as u8;
            addr
        }

        fn sockaddr_un() -> sockaddr_un {
            sockaddr_un::zeroed()
        }

        fn finish_unix(addr: &mut sockaddr_un, len: libc::socklen_t) {
            addr.sun_len = len as u8;
        }

        fn set_no_sigpipe(handle: &Handle) -> io::Result<()> {
            handle.setsockopt_raw(libc::SOL_SOCKET, libc::SO_NOSIGPIPE, 1)
        }

        fn open_pipe() -> io::Result<[OwnedFd; 2]> {
            let mut fds = [0 as RawFd; 2];
            let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                return Err(Error::last_os_error());
            }
            for fd in fds {
                unsafe {
                    libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                    let flags = libc::fcntl(fd, libc::F_GETFL, 0);
                    if flags >= 0 {
                        libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
                    }
                }
            }
            let [read, write] = fds;
            let read = unsafe { OwnedFd::from_raw_fd(read) };
            let write = unsafe { OwnedFd::from_raw_fd(write) };
            Ok([read, write])
        }
    }
}
