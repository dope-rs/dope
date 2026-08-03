use std::io::{self, Error};
use std::net::{SocketAddrV4, SocketAddrV6};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use libc::{
    AF_INET, AF_INET6, in_addr, in6_addr, sockaddr_in, sockaddr_in6, sockaddr_un, socklen_t,
};

use crate::driver::Driver;
use crate::io::ffi::Handle;
use crate::io::pipe::PipeEnds;
use crate::io::socket::Pod;

pub(crate) trait PlatformAbi {
    fn sockaddr_v4() -> sockaddr_in;
    fn sockaddr_v6() -> sockaddr_in6;
    fn sockaddr_un() -> sockaddr_un;
    fn finish_unix(addr: &mut sockaddr_un, len: socklen_t);
    fn set_no_sigpipe(handle: &Handle) -> io::Result<()>;
    fn open_pipe() -> io::Result<PipeEnds>;

    fn encode_v4(addr: SocketAddrV4) -> sockaddr_in {
        let mut encoded = Self::sockaddr_v4();
        encoded.sin_family = AF_INET as _;
        encoded.sin_port = addr.port().to_be();
        encoded.sin_addr = in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        };
        encoded
    }

    fn encode_v6(addr: SocketAddrV6) -> sockaddr_in6 {
        let mut encoded = Self::sockaddr_v6();
        encoded.sin6_family = AF_INET6 as _;
        encoded.sin6_port = addr.port().to_be();
        encoded.sin6_flowinfo = addr.flowinfo();
        encoded.sin6_scope_id = addr.scope_id();
        encoded.sin6_addr = in6_addr {
            s6_addr: addr.ip().octets(),
        };
        encoded
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use libc::{O_CLOEXEC, O_NONBLOCK, pipe2};

    use super::{
        Driver, Error, FromRawFd, Handle, OwnedFd, PipeEnds, PlatformAbi, Pod, RawFd, io,
        sockaddr_in, sockaddr_in6, sockaddr_un, socklen_t,
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

        fn finish_unix(_addr: &mut sockaddr_un, _len: socklen_t) {}

        fn set_no_sigpipe(_handle: &Handle) -> io::Result<()> {
            Ok(())
        }

        fn open_pipe() -> io::Result<PipeEnds> {
            let mut fds = [0 as RawFd; 2];
            let rc = unsafe { pipe2(fds.as_mut_ptr(), O_CLOEXEC | O_NONBLOCK) };
            if rc != 0 {
                return Err(Error::last_os_error());
            }
            let [read, write] = fds;
            let read = unsafe { OwnedFd::from_raw_fd(read) };
            let write = unsafe { OwnedFd::from_raw_fd(write) };
            Ok(PipeEnds::new(read, write))
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod kqueue {
    use std::mem::size_of;

    use libc::{
        F_GETFL, F_SETFD, F_SETFL, FD_CLOEXEC, O_NONBLOCK, SO_NOSIGPIPE, SOL_SOCKET, fcntl, pipe,
    };

    use super::{
        Driver, Error, FromRawFd, Handle, OwnedFd, PipeEnds, PlatformAbi, Pod, RawFd, io,
        sockaddr_in, sockaddr_in6, sockaddr_un, socklen_t,
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

        fn finish_unix(addr: &mut sockaddr_un, len: socklen_t) {
            addr.sun_len = len as u8;
        }

        fn set_no_sigpipe(handle: &Handle) -> io::Result<()> {
            handle.setsockopt_raw(SOL_SOCKET, SO_NOSIGPIPE, 1)
        }

        fn open_pipe() -> io::Result<PipeEnds> {
            let mut fds = [0 as RawFd; 2];
            let rc = unsafe { pipe(fds.as_mut_ptr()) };
            if rc != 0 {
                return Err(Error::last_os_error());
            }
            for fd in fds {
                unsafe {
                    fcntl(fd, F_SETFD, FD_CLOEXEC);
                    let flags = fcntl(fd, F_GETFL, 0);
                    if flags >= 0 {
                        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
                    }
                }
            }
            let [read, write] = fds;
            let read = unsafe { OwnedFd::from_raw_fd(read) };
            let write = unsafe { OwnedFd::from_raw_fd(write) };
            Ok(PipeEnds::new(read, write))
        }
    }
}
