use core::slice;

use crate::io::{socket::msg, transfer};

pub trait Iovec {
    /// Constructs a retained iovec.
    /// # Safety
    /// The bytes stay fixed through terminal completion or quiescence.
    unsafe fn retain(self) -> msg::Iovec;
}

impl Iovec for &[u8] {
    unsafe fn retain(self) -> msg::Iovec {
        msg::Iovec::from_slice(self)
    }
}

pub trait Vectored<'a> {
    /// Constructs a retained descriptor graph.
    /// # Safety
    /// All inputs stay fixed through completion; length is their bounded sum.
    unsafe fn retain(self) -> msg::Vectored<'a>;
}

impl<'a> Vectored<'a> for (&'a mut msg::Header, &'a [msg::Iovec], transfer::Len) {
    unsafe fn retain(self) -> msg::Vectored<'a> {
        let (header, iovecs, bytes) = self;
        debug_assert!(iovecs.len() <= msg::MAX_IOVECS);
        debug_assert_eq!(
            Some(bytes.into_usize()),
            iovecs
                .iter()
                .try_fold(0usize, |total, iovec| total.checked_add(iovec.len()))
        );
        *header = msg::Header::new();
        header.bind_iovs(iovecs);
        msg::Vectored::from_parts(header, iovecs, bytes)
    }
}

pub(super) trait Project<'a> {
    fn slice(self) -> &'a [u8];
}

impl<'a> Project<'a> for &'a msg::Iovec {
    fn slice(self) -> &'a [u8] {
        // SAFETY: construction proves the bytes live for the graph lifetime.
        unsafe { slice::from_raw_parts(self.raw.iov_base.cast(), self.raw.iov_len) }
    }
}

pub struct Part<'a> {
    slice: &'a [u8],
    bytes: transfer::Len,
}

impl<'a> Part<'a> {
    /// # Safety
    /// `slice.len()` must not exceed [`transfer::MAX_BYTES`].
    #[must_use]
    pub unsafe fn from_bounded(slice: &'a [u8]) -> Self {
        debug_assert!(slice.len() <= transfer::MAX_BYTES);
        Self {
            slice,
            bytes: transfer::Len::from_bounded(slice.len()),
        }
    }

    pub(super) fn into_parts(self) -> (&'a [u8], transfer::Len) {
        (self.slice, self.bytes)
    }
}
