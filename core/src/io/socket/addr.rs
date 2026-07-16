use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::path::Path;
use std::{io, slice};

use crate::backend::PlatformAbi;
use crate::driver::Driver;
use crate::io::socket::Pod;
use std::io::{Error, ErrorKind};
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

#[derive(Clone, Copy, Debug)]
pub struct Addr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

#[derive(Clone, Copy)]
#[repr(C)]
union InetStorage {
    v4: libc::sockaddr_in,
    v6: libc::sockaddr_in6,
}

#[derive(Clone, Copy)]
pub struct InetAddr {
    storage: InetStorage,
    len: libc::socklen_t,
}

impl InetAddr {
    pub fn from_std(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => Self {
                storage: InetStorage {
                    v4: Driver::encode_v4(v4),
                },
                len: size_of::<libc::sockaddr_in>() as libc::socklen_t,
            },
            SocketAddr::V6(v6) => Self {
                storage: InetStorage {
                    v6: Driver::encode_v6(v6),
                },
                len: size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            },
        }
    }

    pub fn mut_ptr(&mut self) -> *mut libc::sockaddr {
        &raw mut self.storage as *mut libc::sockaddr
    }

    pub fn socklen(&self) -> libc::socklen_t {
        self.len
    }
}

impl Addr {
    pub fn empty() -> Self {
        Self {
            storage: Pod::zeroed(),
            len: size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        }
    }

    fn from_payload<T: Copy>(payload: T, len: libc::socklen_t) -> Self {
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
        let bytes =
            unsafe { slice::from_raw_parts(&payload as *const T as *const u8, size_of::<T>()) };
        out.storage_bytes()[..bytes.len()].copy_from_slice(bytes);
        out.len = len;
        out
    }

    fn storage_bytes(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(
                &raw mut self.storage as *mut u8,
                size_of::<libc::sockaddr_storage>(),
            )
        }
    }

    pub fn ptr(&self) -> *const libc::sockaddr {
        &raw const self.storage as *const libc::sockaddr
    }

    pub fn mut_ptr(&mut self) -> *mut libc::sockaddr {
        &raw mut self.storage as *mut libc::sockaddr
    }

    pub fn socklen(&self) -> libc::socklen_t {
        self.len
    }

    pub fn len_ptr(&mut self) -> *mut libc::socklen_t {
        &mut self.len as *mut libc::socklen_t
    }

    pub fn from_std(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => Self::from_payload(
                Driver::encode_v4(v4),
                size_of::<libc::sockaddr_in>() as libc::socklen_t,
            ),
            SocketAddr::V6(v6) => Self::from_payload(
                Driver::encode_v6(v6),
                size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            ),
        }
    }

    pub fn from_unix_path(path: &Path) -> io::Result<Self> {
        let (sa, len) = Driver::encode_unix(path)?;
        Ok(Self::from_payload(sa, len))
    }

    pub fn from_getsockname(fd: RawFd) -> io::Result<Self> {
        let mut addr = Self::empty();
        let rc = unsafe { libc::getsockname(fd, addr.mut_ptr(), addr.len_ptr()) };
        if rc < 0 {
            return Err(Error::last_os_error());
        }
        Ok(addr)
    }

    pub fn parse_msg_name(name: &[u8]) -> Option<SocketAddr> {
        if name.len() > size_of::<libc::sockaddr_storage>() {
            return None;
        }
        let mut addr = Self::empty();
        addr.storage_bytes()[..name.len()].copy_from_slice(name);
        addr.into_std_len(name.len() as libc::socklen_t).ok()
    }

    pub fn into_std(self) -> io::Result<SocketAddr> {
        self.into_std_len(self.socklen())
    }

    fn into_std_len(self, len: libc::socklen_t) -> io::Result<SocketAddr> {
        if (len as usize) < size_of::<libc::sa_family_t>() {
            return Err(Error::new(ErrorKind::InvalidData, "short sockaddr"));
        }

        let family = self.storage.ss_family as i32;
        match family {
            libc::AF_INET => {
                if (len as usize) < size_of::<libc::sockaddr_in>() {
                    return Err(Error::new(ErrorKind::InvalidData, "short sockaddr_in"));
                }
                let sa = unsafe { &*self.ptr().cast::<libc::sockaddr_in>() };
                let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
                let port = u16::from_be(sa.sin_port);
                Ok(SocketAddr::new(ip.into(), port))
            }
            libc::AF_INET6 => {
                if (len as usize) < size_of::<libc::sockaddr_in6>() {
                    return Err(Error::new(ErrorKind::InvalidData, "short sockaddr_in6"));
                }
                let sa = unsafe { &*self.ptr().cast::<libc::sockaddr_in6>() };
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
