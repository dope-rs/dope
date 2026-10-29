use std::ptr;

use crate::io::socket::addr;

pub mod source;

/// Exact message flags one receive reported for its payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Flags(u32);

impl Flags {
    pub const NONE: Self = Self(0);

    pub const fn new(bits: u32) -> Self {
        Self(bits)
    }

    /// Whether the kernel truncated the received payload.
    pub const fn is_truncated(self) -> bool {
        self.0 & libc::MSG_TRUNC as u32 != 0
    }

    /// Raw reported bits, for exact diagnostics.
    pub const fn raw(self) -> u32 {
        self.0
    }
}

/// One native scatter/gather extent.
///
/// Values carry no borrow by themselves; installing them in a [`Header`] and
/// freezing that header retains the bytes they point at.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Iovec(pub(super) libc::iovec);

impl Iovec {
    #[must_use]
    pub const fn empty() -> Self {
        Self(libc::iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        })
    }

    #[must_use]
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self(libc::iovec {
            iov_base: bytes.as_ptr().cast_mut().cast(),
            iov_len: bytes.len(),
        })
    }

    /// Exact byte extent this entry covers.
    #[must_use]
    pub const fn len(self) -> usize {
        self.0.iov_len
    }

    pub const fn is_empty(self) -> bool {
        self.len() == 0
    }
}

/// One native message header over caller-owned extents.
///
/// The value is deliberately not `Copy`: a configured header is the root of a
/// raw pointer graph and is frozen through the caller's message type.
#[repr(transparent)]
pub struct Header(libc::msghdr);

impl Header {
    #[must_use]
    pub const fn new() -> Self {
        Self(libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        })
    }

    /// Installs one exact address view as this header's destination.
    pub fn name(&mut self, addr: addr::Addr<'_>) {
        let (addr, len) = addr.parts();
        self.0.msg_name = addr.cast_mut().cast();
        self.0.msg_namelen = len.raw();
    }

    /// Installs ancillary bytes whose borrow the caller retains.
    pub fn control(&mut self, control: &[u8]) {
        self.0.msg_control = control.as_ptr().cast_mut().cast();
        self.0.msg_controllen = control.len() as _;
    }

    /// Installs the exact extent list as this header's scatter/gather vector.
    pub fn bind(&mut self, iovecs: &[Iovec]) {
        self.0.msg_iov = if iovecs.is_empty() {
            ptr::null_mut()
        } else {
            iovecs.as_ptr().cast_mut().cast()
        };
        self.0.msg_iovlen = iovecs.len() as _;
    }

    pub(crate) const fn as_raw(&self) -> &libc::msghdr {
        &self.0
    }
}

impl Default for Header {
    fn default() -> Self {
        Self::new()
    }
}
