use core::{marker, ptr};

use crate::{
    backend,
    io::{socket, transfer},
};

pub mod raw;

/// Maximum number of scatter/gather entries accepted by one socket message.
pub const MAX_IOVECS: usize = <backend::Backend as backend::Socket>::MAX_IOVECS;

/// Address-stable storage for one send iovec.
/// Values carry no borrow by themselves; safe pointer installation goes
/// through [`Builder`] and [`Parts`].
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct Iovec {
    raw: libc::iovec,
}

impl Iovec {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            raw: libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            },
        }
    }

    fn from_slice(buf: &[u8]) -> Self {
        Self {
            raw: libc::iovec {
                iov_base: buf.as_ptr().cast_mut().cast(),
                iov_len: buf.len(),
            },
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.raw.iov_len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw.iov_len == 0
    }
}

/// Address-stable storage for a kernel `msghdr`.
/// The storage is deliberately not `Copy`: a configured header is the root of
/// a raw pointer graph and must be frozen through [`Message`].
#[derive(Debug)]
#[repr(transparent)]
pub struct Header {
    raw: libc::msghdr,
}

impl Header {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            raw: libc::msghdr {
                msg_name: ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: ptr::null_mut(),
                msg_iovlen: 0,
                msg_control: ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            },
        }
    }

    fn bind_iovs(&mut self, iovs: &[Iovec]) {
        self.raw.msg_iov = if iovs.is_empty() {
            ptr::null_mut()
        } else {
            iovs.as_ptr().cast_mut().cast()
        };
        self.raw.msg_iovlen = iovs.len() as _;
    }
}

impl Default for Header {
    fn default() -> Self {
        Self::new()
    }
}

/// A complete immutable send pointer graph.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Message<'a> {
    header: *const libc::msghdr,
    graph: marker::PhantomData<&'a Header>,
}

impl Message<'_> {
    pub(crate) fn raw(self) -> *const libc::msghdr {
        self.header
    }
}

/// A send message together with the iovecs whose bytes it proves live.
pub struct Vectored<'a> {
    message: Message<'a>,
    iovs: &'a [Iovec],
    bytes: transfer::Len,
}

impl<'a> Vectored<'a> {
    #[must_use]
    pub fn message(&self) -> Message<'a> {
        self.message
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        let iovs: &'a [Iovec] = self.iovs;
        iovs.iter().map(raw::Project::slice)
    }

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes.into_usize()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes == transfer::Len::ZERO
    }
}

impl<'a> Vectored<'a> {
    fn from_parts(header: &'a mut Header, iovs: &'a [Iovec], bytes: transfer::Len) -> Self {
        Self {
            message: Message {
                header: ptr::from_ref(&header.raw),
                graph: marker::PhantomData,
            },
            iovs,
            bytes,
        }
    }
}

/// A fixed number of byte slices with their exact bounded total.
pub struct Parts<'a, const N: usize> {
    slices: [&'a [u8]; N],
    bytes: transfer::Len,
}

impl<'a, const N: usize> Parts<'a, N> {
    pub fn try_new(slices: [&'a [u8]; N]) -> Option<Self> {
        if N > MAX_IOVECS {
            return None;
        }
        let mut bytes = transfer::Len::ZERO;
        for slice in &slices {
            bytes = bytes.checked_add(slice.len())?;
        }
        Some(Self { slices, bytes })
    }
}

impl<'a> Parts<'a, 1> {
    #[must_use]
    pub fn single(part: raw::Part<'a>) -> Self {
        let (slice, bytes) = part.into_parts();
        Self {
            slices: [slice],
            bytes,
        }
    }
}

impl<'a> Parts<'a, 2> {
    #[must_use]
    pub fn prefixes(limit: transfer::Len, sources: [&'a [u8]; 2]) -> Self {
        let first_len = sources[0].len().min(limit.into_usize());
        let second_len = sources[1].len().min(limit.into_usize() - first_len);
        let slices = [&sources[0][..first_len], &sources[1][..second_len]];
        Self {
            slices,
            bytes: transfer::Len::from_bounded(first_len + second_len),
        }
    }
}

/// Configures one send header while retaining every installed resource for `'a`.
///
/// Payloads cannot be released while a finished message can still reach the
/// kernel:
///
/// ```compile_fail
/// use dope_core::io::socket::msg::{Header, Iovec, Parts, Builder};
///
/// let mut header = Header::new();
/// let mut iovs = [Iovec::empty()];
/// let message = {
///     let payload = vec![1_u8, 2, 3];
///     let parts = Parts::try_new([payload.as_slice()]).expect("three bytes fit");
///     Builder::new(&mut header).finish(&mut iovs, parts)
/// };
/// drop(message);
/// ```
pub struct Builder<'a> {
    header: &'a mut Header,
    graph: marker::PhantomData<&'a Header>,
}

impl<'a> Builder<'a> {
    pub fn new(header: &'a mut Header) -> Self {
        *header = Header::new();
        Self {
            header,
            graph: marker::PhantomData,
        }
    }

    pub fn name(&mut self, addr: &'a socket::raw::Inet) {
        self.header.raw.msg_name = addr.ptr().cast_mut().cast();
        self.header.raw.msg_namelen = addr.socklen();
    }

    /// Installs ancillary bytes and keeps them borrowed with the whole graph.
    #[doc(hidden)]
    pub fn control(&mut self, control: &'a [u8]) {
        self.header.raw.msg_control = control.as_ptr().cast_mut().cast();
        self.header.raw.msg_controllen = control.len() as _;
    }

    #[must_use]
    pub fn finish<const N: usize>(
        self,
        iovs: &'a mut [Iovec; N],
        parts: Parts<'a, N>,
    ) -> Vectored<'a> {
        let Parts { slices, bytes } = parts;
        *iovs = slices.map(Iovec::from_slice);
        let iovs: &'a [Iovec] = iovs;
        self.header.bind_iovs(iovs);
        Vectored {
            message: Message {
                header: ptr::from_ref(&self.header.raw),
                graph: self.graph,
            },
            iovs,
            bytes,
        }
    }
}

const _: () = assert!(size_of::<Iovec>() == size_of::<libc::iovec>());
const _: () = assert!(align_of::<Iovec>() == align_of::<libc::iovec>());
const _: () = assert!(size_of::<Header>() == size_of::<libc::msghdr>());
const _: () = assert!(align_of::<Header>() == align_of::<libc::msghdr>());
const _: () = assert!(size_of::<Message<'static>>() == size_of::<*const libc::msghdr>());
const _: () = assert!(MAX_IOVECS >= 2);
