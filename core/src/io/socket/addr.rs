use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::path::Path;

use crate::driver::Driver;
use crate::io::socket::Pod;
use crate::platform::raw::abi::PlatformAbi;
use libc::AF_INET;
use libc::AF_INET6;
use libc::getsockname;
use libc::sa_family_t;
use libc::sockaddr;
use libc::sockaddr_in;
use libc::sockaddr_in6;
use libc::sockaddr_storage;
use libc::socklen_t;
use std::io::{Error, ErrorKind};
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::slice::from_raw_parts;
use std::slice::from_raw_parts_mut;

#[derive(Clone, Copy, Debug)]
pub struct Addr {
    storage: sockaddr_storage,
    len: socklen_t,
}

#[derive(Clone, Copy)]
#[repr(C)]
union InetStorage {
    v4: sockaddr_in,
    v6: sockaddr_in6,
}

#[derive(Clone, Copy)]
pub struct InetAddr {
    storage: InetStorage,
    len: socklen_t,
}

impl InetAddr {
    pub fn from_std(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => Self {
                storage: InetStorage {
                    v4: Driver::encode_v4(v4),
                },
                len: size_of::<sockaddr_in>() as socklen_t,
            },
            SocketAddr::V6(v6) => Self {
                storage: InetStorage {
                    v6: Driver::encode_v6(v6),
                },
                len: size_of::<sockaddr_in6>() as socklen_t,
            },
        }
    }

    pub fn mut_ptr(&mut self) -> *mut sockaddr {
        &raw mut self.storage as *mut sockaddr
    }

    pub fn socklen(&self) -> socklen_t {
        self.len
    }
}

impl Addr {
    pub fn empty() -> Self {
        Self {
            storage: Pod::zeroed(),
            len: size_of::<sockaddr_storage>() as socklen_t,
        }
    }

    fn from_payload<T: Copy>(payload: T, len: socklen_t) -> Self {
        const {
            assert!(
                size_of::<T>() <= size_of::<libc::sockaddr_storage>(),
                "payload does not fit in sockaddr_storage",
            );
            assert!(
                align_of::<T>() <= align_of::<libc::sockaddr_storage>(),
                "payload over-aligned for sockaddr_storage",
            );
        }
        let mut out = Self::empty();
        let bytes = unsafe { from_raw_parts(&payload as *const T as *const u8, size_of::<T>()) };
        out.storage_bytes()[..bytes.len()].copy_from_slice(bytes);
        out.len = len;
        out
    }

    fn storage_bytes(&mut self) -> &mut [u8] {
        unsafe {
            from_raw_parts_mut(
                &raw mut self.storage as *mut u8,
                size_of::<sockaddr_storage>(),
            )
        }
    }

    pub fn ptr(&self) -> *const sockaddr {
        &raw const self.storage as *const sockaddr
    }

    pub fn mut_ptr(&mut self) -> *mut sockaddr {
        &raw mut self.storage as *mut sockaddr
    }

    pub fn socklen(&self) -> socklen_t {
        self.len
    }

    pub fn len_ptr(&mut self) -> *mut socklen_t {
        &mut self.len as *mut socklen_t
    }

    pub fn from_std(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => {
                Self::from_payload(Driver::encode_v4(v4), size_of::<sockaddr_in>() as socklen_t)
            }
            SocketAddr::V6(v6) => Self::from_payload(
                Driver::encode_v6(v6),
                size_of::<sockaddr_in6>() as socklen_t,
            ),
        }
    }

    pub fn from_unix_path(path: &Path) -> io::Result<Self> {
        let (sa, len) = Driver::encode_unix(path)?;
        Ok(Self::from_payload(sa, len))
    }

    pub fn from_getsockname(fd: RawFd) -> io::Result<Self> {
        let mut addr = Self::empty();
        let rc = unsafe { getsockname(fd, addr.mut_ptr(), addr.len_ptr()) };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        Ok(addr)
    }

    pub fn parse_msg_name(name: &[u8]) -> Option<SocketAddr> {
        if name.len() > size_of::<sockaddr_storage>() {
            return None;
        }
        let mut addr = Self::empty();
        addr.storage_bytes()[..name.len()].copy_from_slice(name);
        addr.into_std_len(name.len() as socklen_t).ok()
    }

    pub fn into_std(self) -> io::Result<SocketAddr> {
        self.into_std_len(self.socklen())
    }

    fn into_std_len(self, len: socklen_t) -> io::Result<SocketAddr> {
        if (len as usize) < size_of::<sa_family_t>() {
            return Err(Error::new(ErrorKind::InvalidData, "short sockaddr"));
        }

        let family = self.storage.ss_family as i32;
        match family {
            AF_INET => {
                if (len as usize) < size_of::<sockaddr_in>() {
                    return Err(Error::new(ErrorKind::InvalidData, "short sockaddr_in"));
                }
                let sa = unsafe { &*self.ptr().cast::<sockaddr_in>() };
                let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
                let port = u16::from_be(sa.sin_port);
                Ok(SocketAddr::new(ip.into(), port))
            }
            AF_INET6 => {
                if (len as usize) < size_of::<sockaddr_in6>() {
                    return Err(Error::new(ErrorKind::InvalidData, "short sockaddr_in6"));
                }
                let sa = unsafe { &*self.ptr().cast::<sockaddr_in6>() };
                let ip = Ipv6Addr::from(sa.sin6_addr.s6_addr);
                let port = u16::from_be(sa.sin6_port);
                Ok(SocketAddr::new(ip.into(), port))
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "unknown sockaddr family",
            )),
        }
    }
}
