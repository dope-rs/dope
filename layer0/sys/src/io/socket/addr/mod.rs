//! Native socket addresses.

use std::{
    marker, mem, net, num,
    os::fd::{AsRawFd as _, BorrowedFd},
    ptr,
};

use crate::{Errno, io::socket::Domain, kernel};

mod internet;
pub use internet::Internet;

/// Exact byte capacity of one native socket-address buffer.
pub const CAPACITY: usize = mem::size_of::<libc::sockaddr_storage>();

/// Exact byte length of the namespace prefix every address carries.
pub const FAMILY_BYTES: usize =
    mem::offset_of!(libc::sockaddr_storage, ss_family) + mem::size_of::<libc::sa_family_t>();

/// A validated nonzero native socket-address length.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Socklen(num::NonZero<libc::socklen_t>);

impl Socklen {
    /// Exact length of one admitted address payload.
    const fn of<T>() -> Self {
        const {
            assert!(
                mem::size_of::<T>() > 0 && mem::size_of::<T>() <= CAPACITY,
                "address payload does not fit sockaddr_storage",
            );
        }
        Self(
            num::NonZero::<libc::socklen_t>::MIN
                .saturating_add(mem::size_of::<T>() as libc::socklen_t - 1),
        )
    }

    /// Validates one observed length against the exact storage capacity.
    fn try_from_len(len: usize) -> Option<Self> {
        if len > CAPACITY {
            return None;
        }
        num::NonZero::new(len as libc::socklen_t).map(Self)
    }

    pub const fn get(self) -> usize {
        self.0.get() as usize
    }

    pub(crate) const fn raw(self) -> libc::socklen_t {
        self.0.get()
    }
}

/// Native encoding of the two-byte address header selected by its backend.
pub trait Source {
    /// Whether every pathname must retain a trailing NUL inside the path field.
    const UNIX_TERMINATED: bool;
    /// Whether a leading NUL selects the native abstract namespace.
    const UNIX_ABSTRACT: bool;

    fn header(domain: Domain, len: Socklen) -> [u8; 2];
}

/// One native Unix-domain socket address with its exact path length.
#[derive(Clone, Copy)]
pub struct Unix {
    raw: libc::sockaddr_un,
    len: Socklen,
}

enum Name {
    Path,
    Abstract,
}

impl Unix {
    /// Encodes one pathname, proving it fits the platform's path field.
    pub fn from_path<S: Source>(bytes: &[u8]) -> Result<Self, PathError> {
        Self::encode::<S>(bytes, Name::Path)
    }

    /// Encodes an abstract name without adding a trailing NUL to its identity.
    pub fn from_name<S: Source>(bytes: &[u8]) -> Result<Self, PathError> {
        Self::encode::<S>(bytes, Name::Abstract)
    }

    fn encode<S: Source>(bytes: &[u8], name: Name) -> Result<Self, PathError> {
        // SAFETY: zero initializes every native address field and path byte.
        let mut raw: libc::sockaddr_un = unsafe { mem::zeroed() };
        let capacity = raw.sun_path.len();
        let (prefix, suffix) = match name {
            Name::Path => {
                PathError::check(bytes, capacity - usize::from(S::UNIX_TERMINATED))?;
                (0, usize::from(bytes.len() < capacity))
            }
            Name::Abstract => {
                if !S::UNIX_ABSTRACT {
                    return Err(PathError::Unsupported);
                }
                PathError::length(bytes.len(), capacity - 1)?;
                (1, 0)
            }
        };
        for (dst, byte) in raw.sun_path.iter_mut().skip(prefix).zip(bytes) {
            *dst = *byte as libc::c_char;
        }
        let len = num::NonZero::<libc::socklen_t>::MIN.saturating_add(
            (mem::offset_of!(libc::sockaddr_un, sun_path) + prefix + bytes.len() + suffix - 1)
                as libc::socklen_t,
        );
        let len = Socklen(len);
        // SAFETY: the native header occupies the first two initialized bytes.
        unsafe {
            ptr::from_mut(&mut raw)
                .cast::<[u8; 2]>()
                .write(S::header(Domain::Unix, len))
        };
        Ok(Self { raw, len })
    }

    /// Typed view of this address for one syscall.
    #[must_use]
    pub fn as_addr(&self) -> Addr<'_> {
        Addr::new(&self.raw, self.len)
    }
}

/// An address length the kernel may overwrite in place.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Length(libc::socklen_t);

/// One native address buffer with its exact length.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Storage<L = Length> {
    storage: libc::sockaddr_storage,
    len: L,
}

impl Storage<Length> {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            storage: unsafe { mem::zeroed() },
            len: Length(CAPACITY as libc::socklen_t),
        }
    }

    /// Exclusive view of the buffer and the length the kernel overwrites.
    pub fn output_mut(&mut self) -> (&mut libc::sockaddr_storage, &mut libc::socklen_t) {
        (&mut self.storage, &mut self.len.0)
    }
}

impl Storage<Socklen> {
    #[must_use]
    pub fn as_addr(&self) -> Addr<'_> {
        Addr::new(&self.storage, self.len)
    }

    /// Copies one exact native address view into a fresh buffer.
    #[must_use]
    pub fn from_addr(source: Addr<'_>) -> Self {
        let (addr, len) = source.parts();
        let mut out = Self {
            storage: unsafe { mem::zeroed() },
            len,
        };
        // SAFETY: the borrowed view retains its complete native payload; the
        // admitted length fits this distinct destination without losing padding.
        unsafe {
            ptr::copy_nonoverlapping(
                addr.cast::<u8>(),
                ptr::from_mut(&mut out.storage).cast(),
                len.get(),
            )
        };
        out
    }
}

impl<L> Storage<L> {
    fn decode(&self, actual: usize) -> Result<net::SocketAddr, Error> {
        // SAFETY: both callers admit actual <= CAPACITY; this immutable
        // native storage retains its initialized address fields.
        unsafe { Internet::decode(ptr::from_ref(&self.storage).cast(), actual) }
    }
}

const _: () = {
    assert!(CAPACITY <= libc::socklen_t::MAX as usize);
    let fields = CAPACITY + mem::size_of::<libc::socklen_t>();
    let align = mem::align_of::<Storage<Socklen>>();
    assert!(mem::size_of::<Storage<Socklen>>() == fields.div_ceil(align) * align);
};

/// Exact native address decoding failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Storage {
        actual: usize,
        capacity: usize,
    },
    Header {
        actual: usize,
        required: usize,
    },
    Address {
        family: i32,
        actual: usize,
        required: usize,
    },
    Unsupported {
        family: i32,
        actual: usize,
    },
}

impl Storage<Socklen> {
    pub fn to_std(&self) -> Result<net::SocketAddr, Error> {
        self.decode(self.len.get())
    }
}

impl Storage<Length> {
    pub fn to_std(&self) -> Result<net::SocketAddr, Error> {
        let actual = self.len.0 as usize;
        if actual > CAPACITY {
            return Err(Error::Storage {
                actual,
                capacity: CAPACITY,
            });
        }
        self.decode(actual)
    }
}

/// Exact Unix-domain name validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathError {
    Unsupported,
    Empty,
    Length { actual: usize, maximum: usize },
    Nul { offset: usize },
}

impl PathError {
    fn check(bytes: &[u8], maximum: usize) -> Result<(), Self> {
        if bytes.is_empty() {
            return Err(Self::Empty);
        }
        Self::length(bytes.len(), maximum)?;
        if let Some(offset) = bytes.iter().position(|byte| *byte == 0) {
            return Err(Self::Nul { offset });
        }
        Ok(())
    }

    fn length(actual: usize, maximum: usize) -> Result<(), Self> {
        if actual > maximum {
            return Err(Self::Length { actual, maximum });
        }
        Ok(())
    }
}

/// Exact native address extent borrowed from its immutable owner.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Addr<'a> {
    addr: ptr::NonNull<libc::sockaddr>,
    len: Socklen,
    owner: marker::PhantomData<&'a libc::sockaddr>,
}

/// Exact failure while observing one descriptor's local address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Local<E> {
    Syscall(E),
    Length { actual: usize, capacity: usize },
}

impl<'a> Addr<'a> {
    fn new<T>(storage: &'a T, len: Socklen) -> Self {
        Self {
            addr: ptr::NonNull::from(storage).cast(),
            len,
            owner: marker::PhantomData,
        }
    }

    pub const fn parts(self) -> (*const libc::sockaddr, Socklen) {
        (self.addr.as_ptr().cast_const(), self.len)
    }

    pub fn local<S: kernel::raw::Source + ?Sized, E: From<Errno>>(
        file: BorrowedFd<'_>,
        storage: &'a mut Storage<Length>,
    ) -> Result<Self, Local<E>> {
        let (address, length) = storage.output_mut();
        *length = CAPACITY as libc::socklen_t;
        // SAFETY: the exclusive address and length borrows retain their full
        // capacity through getsockname, which returns zero or minus one.
        let status =
            unsafe { libc::getsockname(file.as_raw_fd(), ptr::from_mut(address).cast(), length) };
        // SAFETY: getsockname returns zero or -1; failure reads its unchanged errno.
        unsafe {
            kernel::raw::Status::new(status).try_zero::<E>(|| kernel::raw::Code::last::<S>().into())
        }
        .map_err(Local::Syscall)?;
        let actual = *length as usize;
        let len = Socklen::try_from_len(actual).ok_or(Local::Length {
            actual,
            capacity: CAPACITY,
        })?;
        Ok(Self::new(&storage.storage, len))
    }

    pub fn try_bind<S: kernel::raw::Source + ?Sized, E: From<Errno>>(
        self,
        file: BorrowedFd<'_>,
    ) -> Result<(), E> {
        // SAFETY: the borrowed native payload retains its exact admitted extent.
        let status = unsafe { libc::bind(file.as_raw_fd(), self.addr.as_ptr(), self.len.raw()) };
        // SAFETY: bind returns zero or -1; failure reads its unchanged errno.
        unsafe {
            kernel::raw::Status::new(status).try_zero(|| kernel::raw::Code::last::<S>().into())
        }
    }

    pub fn try_connect<S: kernel::raw::Source + ?Sized, E: From<Errno>>(
        self,
        file: BorrowedFd<'_>,
    ) -> Result<(), E> {
        // SAFETY: the borrowed native payload retains its exact admitted extent.
        let status = unsafe { libc::connect(file.as_raw_fd(), self.addr.as_ptr(), self.len.raw()) };
        // SAFETY: connect returns zero or -1; failure reads its unchanged errno.
        unsafe {
            kernel::raw::Status::new(status).try_zero(|| kernel::raw::Code::last::<S>().into())
        }
    }
}
