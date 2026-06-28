use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::RawFd;
use std::path::Path;
use std::{io, slice};

use crate::backend::sockaddr::{Pod, Stamp};

fn sockaddr_in_of(v4: SocketAddrV4) -> libc::sockaddr_in {
    let mut sa = libc::sockaddr_in::zeroed();
    sa.sin_family = libc::AF_INET as _;
    sa.sin_port = v4.port().to_be();
    sa.sin_addr = libc::in_addr {
        s_addr: u32::from_ne_bytes(v4.ip().octets()),
    };
    sa.stamp();
    sa
}

fn sockaddr_in6_of(v6: SocketAddrV6) -> libc::sockaddr_in6 {
    let mut sa = libc::sockaddr_in6::zeroed();
    sa.sin6_family = libc::AF_INET6 as _;
    sa.sin6_port = v6.port().to_be();
    sa.sin6_flowinfo = v6.flowinfo();
    sa.sin6_scope_id = v6.scope_id();
    sa.sin6_addr = libc::in6_addr {
        s6_addr: v6.ip().octets(),
    };
    sa.stamp();
    sa
}

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

/// IP-only sockaddr (28-byte `sockaddr_in6` worst case, no `sockaddr_storage`
/// padding) for hot UDP send paths where the destination is always V4/V6.
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
                    v4: sockaddr_in_of(v4),
                },
                len: size_of::<libc::sockaddr_in>() as libc::socklen_t,
            },
            SocketAddr::V6(v6) => Self {
                storage: InetStorage {
                    v6: sockaddr_in6_of(v6),
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
            storage: crate::backend::sockaddr::Pod::zeroed(),
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
        // SAFETY: callers pass only POD sockaddr structs; the byte count is bounded by the const assert above.
        let bytes =
            unsafe { slice::from_raw_parts(&payload as *const T as *const u8, size_of::<T>()) };
        out.storage_bytes()[..bytes.len()].copy_from_slice(bytes);
        out.len = len;
        out
    }

    fn storage_bytes(&mut self) -> &mut [u8] {
        // SAFETY: sockaddr_storage is a POD aggregate; its size_of bytes are an addressable, writable region.
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
                sockaddr_in_of(v4),
                size_of::<libc::sockaddr_in>() as libc::socklen_t,
            ),
            SocketAddr::V6(v6) => Self::from_payload(
                sockaddr_in6_of(v6),
                size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            ),
        }
    }

    pub fn from_unix_path(path: &Path) -> io::Result<Self> {
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
        }
        let mut sa = libc::sockaddr_un::zeroed();
        sa.sun_family = libc::AF_UNIX as _;
        let max = sa.sun_path.len().saturating_sub(1);
        if bytes.len() > max {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "path too long"));
        }
        for (i, b) in bytes.iter().enumerate() {
            sa.sun_path[i] = *b as libc::c_char;
        }
        sa.stamp();
        let len = (size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;
        Ok(Self::from_payload(sa, len))
    }

    pub fn from_getsockname(fd: RawFd) -> io::Result<Self> {
        let mut addr = Self::empty();
        // SAFETY: caller-provided fd is live; addr provides writable sockaddr_storage plus its length cell.
        let rc = unsafe { libc::getsockname(fd, addr.mut_ptr(), addr.len_ptr()) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(addr)
    }

    pub fn parse_msg_name(name: &[u8]) -> Option<SocketAddr> {
        if name.len() > size_of::<libc::sockaddr_storage>() {
            return None;
        }
        let mut addr = Self::empty();
        addr.storage_bytes()[..name.len()].copy_from_slice(name);
        addr.to_std_len(name.len() as libc::socklen_t).ok()
    }

    pub fn to_std(&self) -> io::Result<SocketAddr> {
        self.to_std_len(self.socklen())
    }

    fn to_std_len(self, len: libc::socklen_t) -> io::Result<SocketAddr> {
        if (len as usize) < size_of::<libc::sa_family_t>() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short sockaddr"));
        }

        let family = self.storage.ss_family as i32;
        match family {
            libc::AF_INET => {
                if (len as usize) < size_of::<libc::sockaddr_in>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "short sockaddr_in",
                    ));
                }
                // SAFETY: family is AF_INET and len >= size_of::<sockaddr_in>() (checked above).
                let sa = unsafe { &*self.ptr().cast::<libc::sockaddr_in>() };
                let ip = std::net::Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
                let port = u16::from_be(sa.sin_port);
                Ok(SocketAddr::new(ip.into(), port))
            }
            libc::AF_INET6 => {
                if (len as usize) < size_of::<libc::sockaddr_in6>() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "short sockaddr_in6",
                    ));
                }
                // SAFETY: family is AF_INET6 and len >= size_of::<sockaddr_in6>() (checked above).
                let sa = unsafe { &*self.ptr().cast::<libc::sockaddr_in6>() };
                let ip = std::net::Ipv6Addr::from(sa.sin6_addr.s6_addr);
                let port = u16::from_be(sa.sin6_port);
                Ok(SocketAddr::new(ip.into(), port))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown sockaddr family",
            )),
        }
    }
}
