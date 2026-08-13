use std::{
    io, mem, net,
    os::{fd, unix},
    path,
};

use crate::backend;

/// A plain socket-address payload that fits into `sockaddr_storage`.
/// # Safety
/// Implementors must be initialized, byte-copyable C addresses accepted by `Addr`.
unsafe trait Payload: Copy {}

unsafe impl Payload for libc::sockaddr_in {}
unsafe impl Payload for libc::sockaddr_in6 {}
unsafe impl Payload for libc::sockaddr_un {}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Addr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl Addr {
    pub(crate) const STORAGE_CAPACITY: usize = size_of::<libc::sockaddr_storage>();
    pub(crate) const STORAGE_SOCKLEN: libc::socklen_t = {
        assert!(Self::STORAGE_CAPACITY <= libc::socklen_t::MAX as usize);
        Self::STORAGE_CAPACITY as libc::socklen_t
    };

    pub(crate) fn empty() -> Self {
        Self {
            storage: unsafe { mem::zeroed() },
            len: Self::STORAGE_SOCKLEN,
        }
    }

    fn from_payload<T: Payload>(payload: T, len: libc::socklen_t) -> Self {
        const {
            assert!(
                size_of::<T>() <= Self::STORAGE_CAPACITY,
                "payload does not fit in sockaddr_storage",
            );
            assert!(
                align_of::<T>() <= align_of::<libc::sockaddr_storage>(),
                "payload over-aligned for sockaddr_storage",
            );
        }
        let mut out = Self::empty();
        let bytes = unsafe {
            use std::slice::from_raw_parts;
            from_raw_parts(&payload as *const T as *const u8, size_of::<T>())
        };
        out.storage_bytes()[..bytes.len()].copy_from_slice(bytes);
        out.len = len;
        out
    }

    fn storage_bytes(&mut self) -> &mut [u8] {
        unsafe {
            use std::slice::from_raw_parts_mut;
            from_raw_parts_mut(&raw mut self.storage as *mut u8, Self::STORAGE_CAPACITY)
        }
    }

    pub(crate) fn ptr(&self) -> *const libc::sockaddr {
        &raw const self.storage as *const libc::sockaddr
    }

    pub(crate) fn mut_ptr(&mut self) -> *mut libc::sockaddr {
        &raw mut self.storage as *mut libc::sockaddr
    }

    pub(crate) fn socklen(&self) -> libc::socklen_t {
        self.len
    }

    pub(in crate::io::socket) fn family(&self) -> libc::c_int {
        self.storage.ss_family as libc::c_int
    }

    pub(crate) fn len_ptr(&mut self) -> *mut libc::socklen_t {
        &mut self.len as *mut libc::socklen_t
    }

    pub(crate) fn from_std(addr: net::SocketAddr) -> Self {
        match addr {
            net::SocketAddr::V4(v4) => Self::from_payload(
                <backend::Backend as backend::Socket>::encode_v4(v4),
                size_of::<libc::sockaddr_in>() as libc::socklen_t,
            ),
            net::SocketAddr::V6(v6) => Self::from_payload(
                <backend::Backend as backend::Socket>::encode_v6(v6),
                size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            ),
        }
    }

    pub(crate) fn from_unix_path(path: &path::Path) -> io::Result<Self> {
        let bytes = unix::ffi::OsStrExt::as_bytes(path.as_os_str());
        let (encoded, len) = <backend::Backend as backend::Socket>::encode_unix(bytes)?;
        Ok(Self::from_payload(encoded, len))
    }

    pub(crate) fn from_getsockname(fd: fd::RawFd) -> io::Result<Self> {
        let mut addr = Self::empty();
        let rc = unsafe {
            use libc::getsockname;
            getsockname(fd, addr.mut_ptr(), addr.len_ptr())
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(addr)
    }

    pub(crate) fn parse_msg_name(name: &[u8]) -> Option<net::SocketAddr> {
        let mut addr = Self::empty();
        addr.storage_bytes()[..name.len()].copy_from_slice(name);
        addr.into_std_len(name.len() as libc::socklen_t).ok()
    }

    pub(crate) fn into_std(self) -> io::Result<net::SocketAddr> {
        self.into_std_len(self.socklen())
    }

    fn into_std_len(self, len: libc::socklen_t) -> io::Result<net::SocketAddr> {
        if (len as usize) < size_of::<libc::sa_family_t>() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short sockaddr"));
        }

        let family = self.storage.ss_family as i32;
        match family {
            libc::AF_INET => {
                use std::net::Ipv4Addr;
                if (len as usize) < size_of::<libc::sockaddr_in>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "short sockaddr_in",
                    ));
                }
                let sa = unsafe { &*self.ptr().cast::<libc::sockaddr_in>() };
                let ip = Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
                let port = u16::from_be(sa.sin_port);
                Ok(net::SocketAddr::new(ip.into(), port))
            }
            libc::AF_INET6 => {
                use std::net::Ipv6Addr;
                if (len as usize) < size_of::<libc::sockaddr_in6>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "short sockaddr_in6",
                    ));
                }
                let sa = unsafe { &*self.ptr().cast::<libc::sockaddr_in6>() };
                let ip = Ipv6Addr::from(sa.sin6_addr.s6_addr);
                let port = u16::from_be(sa.sin6_port);
                Ok(net::SocketAddr::V6(net::SocketAddrV6::new(
                    ip,
                    port,
                    sa.sin6_flowinfo,
                    sa.sin6_scope_id,
                )))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown sockaddr family",
            )),
        }
    }
}
