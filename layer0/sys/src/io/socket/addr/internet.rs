use std::{mem, net, ptr};

use crate::io::socket::{
    Domain,
    addr::{Addr, CAPACITY, Error, FAMILY_BYTES, Socklen, Source},
};

/// One native IPv4 socket address.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Inet(libc::sockaddr_in);

impl Inet {
    /// Exact byte length of one IPv4 address payload.
    pub(super) const BYTES: usize = mem::size_of::<libc::sockaddr_in>();

    fn from_std<S: Source>(addr: net::SocketAddrV4) -> Self {
        // SAFETY: zero initializes every byte of one platform socket address.
        let mut raw: libc::sockaddr_in = unsafe { mem::zeroed() };
        raw.sin_port = addr.port().to_be();
        raw.sin_addr = libc::in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        };
        // SAFETY: the native header occupies the first two initialized bytes.
        unsafe {
            ptr::from_mut(&mut raw)
                .cast::<[u8; 2]>()
                .write(S::header(Domain::Inet4, Socklen::of::<Self>()))
        };

        Self(raw)
    }

    #[must_use]
    fn to_std(self) -> net::SocketAddrV4 {
        let ip = net::Ipv4Addr::from(u32::from_be(self.0.sin_addr.s_addr));
        net::SocketAddrV4::new(ip, u16::from_be(self.0.sin_port))
    }
}

/// One native IPv6 socket address.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Inet6(libc::sockaddr_in6);

impl Inet6 {
    fn from_std<S: Source>(addr: net::SocketAddrV6) -> Self {
        // SAFETY: zero initializes every byte of one platform socket address.
        let mut raw: libc::sockaddr_in6 = unsafe { mem::zeroed() };
        raw.sin6_port = addr.port().to_be();
        raw.sin6_flowinfo = addr.flowinfo();
        raw.sin6_addr = libc::in6_addr {
            s6_addr: addr.ip().octets(),
        };
        raw.sin6_scope_id = addr.scope_id();
        // SAFETY: the native header occupies the first two initialized bytes.
        unsafe {
            ptr::from_mut(&mut raw)
                .cast::<[u8; 2]>()
                .write(S::header(Domain::Inet6, Socklen::of::<Self>()))
        };

        Self(raw)
    }

    #[must_use]
    fn to_std(self) -> net::SocketAddrV6 {
        net::SocketAddrV6::new(
            net::Ipv6Addr::from(self.0.sin6_addr.s6_addr),
            u16::from_be(self.0.sin6_port),
            self.0.sin6_flowinfo,
            self.0.sin6_scope_id,
        )
    }
}

/// Compact IPv4 or IPv6 storage retaining its selected native length.
#[derive(Clone, Copy)]
pub struct Internet {
    storage: Storage,
    len: Socklen,
}

#[derive(Clone, Copy)]
#[repr(C)]
union Storage {
    v4: Inet,
    v6: Inet6,
}

impl Internet {
    /// Decodes one native address without copying its borrowed storage.
    pub fn try_parse(bytes: &[u8]) -> Result<net::SocketAddr, Error> {
        let actual = bytes.len();
        if actual > CAPACITY {
            return Err(Error::Storage {
                actual,
                capacity: CAPACITY,
            });
        }
        // SAFETY: the immutable slice retains its initialized, admitted extent.
        unsafe { Self::decode(bytes.as_ptr(), actual) }
    }

    /// # Safety
    /// `bytes` must retain `actual` immutable native-address bytes, `actual <= CAPACITY`,
    /// with initialized native fields; native ABI padding need not be initialized.
    pub(super) unsafe fn decode(bytes: *const u8, actual: usize) -> Result<net::SocketAddr, Error> {
        if actual < FAMILY_BYTES {
            return Err(Error::Header {
                actual,
                required: FAMILY_BYTES,
            });
        }
        // SAFETY: header admission covers the native family field;
        // unaligned input is accepted without exposing native padding.
        let family = unsafe {
            bytes
                .add(mem::offset_of!(libc::sockaddr_storage, ss_family))
                .cast::<libc::sa_family_t>()
                .read_unaligned()
        } as libc::c_int;

        // SAFETY: both native payloads have integer-only fields. `read`
        // checks their full extent before reading either selected payload.
        Ok(unsafe {
            match family {
                libc::AF_INET => read::<Inet>(bytes, actual, family)?.to_std().into(),
                libc::AF_INET6 => read::<Inet6>(bytes, actual, family)?.to_std().into(),
                other => {
                    return Err(Error::Unsupported {
                        family: other,
                        actual,
                    });
                }
            }
        })
    }

    pub fn from_std<S: Source>(address: net::SocketAddr) -> Self {
        match address {
            net::SocketAddr::V4(address) => Self {
                storage: Storage {
                    v4: Inet::from_std::<S>(address),
                },
                len: Socklen::of::<Inet>(),
            },
            net::SocketAddr::V6(address) => Self {
                storage: Storage {
                    v6: Inet6::from_std::<S>(address),
                },
                len: Socklen::of::<Inet6>(),
            },
        }
    }

    pub fn as_addr(&self) -> Addr<'_> {
        Addr::new(&self.storage, self.len)
    }

    pub fn domain(&self) -> Domain {
        if self.len.get() == Inet::BYTES {
            Domain::Inet4
        } else {
            Domain::Inet6
        }
    }
}

/// # Safety
/// `bytes` must retain `actual` bytes. If the extent fits `T`, its initialized
/// fields must form a valid `T`; native padding is never exposed as bytes.
unsafe fn read<T: Copy>(bytes: *const u8, actual: usize, family: i32) -> Result<T, Error> {
    let required = mem::size_of::<T>();
    if actual < required {
        return Err(Error::Address {
            family,
            actual,
            required,
        });
    }
    // SAFETY: admission proves the complete caller-validated native payload.
    Ok(unsafe { bytes.cast::<T>().read_unaligned() })
}

const _: () = assert!(mem::size_of::<Internet>() <= 32);
