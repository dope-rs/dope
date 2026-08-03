use std::ops::{Deref, DerefMut};
use std::slice::from_raw_parts;

use dope_core::io::socket::msg::{IoVec, MsgHdr};
use o3::buffer::RollingBuffer;

#[derive(Clone, Copy)]
pub struct Sent(u32);

impl Sent {
    pub(crate) const fn new(bytes: u32) -> Self {
        Self(bytes)
    }

    pub const fn get(self) -> usize {
        self.0 as usize
    }

    #[doc(hidden)]
    pub fn try_from_submission(bytes: usize, submitted: usize) -> Option<Self> {
        if bytes > submitted {
            return None;
        }
        Some(Self(u32::try_from(bytes).ok()?))
    }
}

pub struct SendBuf<const CAP: usize> {
    buf: Box<RollingBuffer<CAP>>,
}

impl<const CAP: usize> SendBuf<CAP> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: RollingBuffer::new_boxed(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[must_use]
    pub fn spare_capacity(&self) -> usize {
        self.buf.spare_capacity()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    pub fn try_extend_from_slice(&mut self, src: &[u8]) -> bool {
        self.buf.try_extend_from_slice(src).is_ok()
    }

    pub fn try_consume(&mut self, n: usize) -> bool {
        self.buf.try_consume(n).is_ok()
    }
}

impl<const CAP: usize> Default for SendBuf<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable storage consulted by the send state machine.
///
/// The returned slice is borrowed from `self`, so Rust already prevents the
/// storage from being mutably accessed while a prepared send borrows it. No
/// unsafe implementation contract is required.
pub trait SendStorage: 'static {
    fn as_slice(&self) -> &[u8];
}

impl<const CAP: usize> SendStorage for SendBuf<CAP> {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl SendStorage for () {
    fn as_slice(&self) -> &[u8] {
        &[]
    }
}

pub struct Storage<'a, S: SendStorage> {
    storage: &'a mut S,
    limit: usize,
}

impl<'a, S: SendStorage> Storage<'a, S> {
    #[doc(hidden)]
    pub fn new(storage: &'a mut S, limit: usize) -> Self {
        Self { storage, limit }
    }

    pub fn buffered(self, consumed: usize) -> Prepared<'a> {
        let consumed = consumed.min(self.limit);
        let bytes = self.storage.as_slice();
        if bytes.is_empty() {
            Prepared {
                payload: Payload::Empty,
                consumed,
                close_after: false,
            }
        } else {
            Prepared {
                payload: Payload::Single(Plain::proven(bytes)),
                consumed,
                close_after: false,
            }
        }
    }

    pub fn empty(self, consumed: usize) -> Prepared<'a> {
        Prepared {
            payload: Payload::Empty,
            consumed: consumed.min(self.limit),
            close_after: false,
        }
    }
}

impl<S: SendStorage> Deref for Storage<'_, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.storage
    }
}

impl<S: SendStorage> DerefMut for Storage<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.storage
    }
}

pub struct Plain<'a> {
    bytes: &'a [u8],
}

/// An owner-backed source for one direct send.
/// # Safety
/// Returned bytes remain live, fixed, and immutable through send completion.
#[doc(hidden)]
pub unsafe trait StablePlainSource<'a> {
    fn into_slice(self) -> &'a [u8];
}

impl<'a> Plain<'a> {
    fn proven(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Converts an owner-level stability proof into a direct-send view.
    #[doc(hidden)]
    pub fn from_stable(source: impl StablePlainSource<'a>) -> Self {
        Self {
            bytes: source.into_slice(),
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &'a [u8] {
        self.bytes
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

pub struct Vectored<'a> {
    iovs: &'a [IoVec],
    iov_storage: &'a mut [IoVec],
    msghdr_storage: &'a mut MsgHdr,
}

/// An owner-backed source for one vectored send.
/// # Safety
/// Bytes stay stable; descriptor and header storage stays live, exclusive, and sufficiently sized through completion.
#[doc(hidden)]
pub unsafe trait StableVectoredSource<'a> {
    fn into_parts(self) -> (&'a [IoVec], &'a mut [IoVec], &'a mut MsgHdr);
}

impl<'a> Vectored<'a> {
    /// Converts an owner-level stability proof into the wire view consumed by
    /// the send path.
    #[doc(hidden)]
    #[inline(always)]
    pub fn from_stable(source: impl StableVectoredSource<'a>) -> Self {
        let (iovs, iov_storage, msghdr_storage) = source.into_parts();
        Self {
            iovs,
            iov_storage,
            msghdr_storage,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        self.iovs
            .iter()
            .map(|iov| unsafe { from_raw_parts(iov.as_ptr(), iov.len()) })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.iovs.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.iovs.is_empty()
    }

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.iovs.iter().map(IoVec::len).sum()
    }

    pub(crate) fn install(&mut self) {
        let n = self.iovs.len();
        self.iov_storage[..n].copy_from_slice(self.iovs);
        self.msghdr_storage.set_iov(&self.iov_storage[..n]);
    }

    pub(crate) fn msghdr(&self) -> &MsgHdr {
        self.msghdr_storage
    }
}

pub(crate) enum Payload<'a> {
    Empty,
    Single(Plain<'a>),
    Vectored(Vectored<'a>),
}

#[must_use]
pub struct Prepared<'a> {
    payload: Payload<'a>,
    consumed: usize,
    close_after: bool,
}

impl<'a> Prepared<'a> {
    pub(crate) fn empty(consumed: usize) -> Self {
        Self {
            payload: Payload::Empty,
            consumed,
            close_after: false,
        }
    }

    pub fn input(plain: Plain<'a>, consumed: usize) -> Self {
        let consumed = consumed.min(plain.len());
        Self {
            payload: Payload::Single(plain),
            consumed,
            close_after: false,
        }
    }

    pub fn vectored(plain: Vectored<'a>, consumed: usize) -> Self {
        let consumed = consumed.min(plain.bytes());
        Self {
            payload: Payload::Vectored(plain),
            consumed,
            close_after: false,
        }
    }

    pub fn static_slice(buf: &'static [u8]) -> Self {
        Self {
            payload: Payload::Single(Plain::proven(buf)),
            consumed: 0,
            close_after: false,
        }
    }

    pub fn close_after(mut self) -> Self {
        self.close_after = true;
        self
    }

    pub(crate) fn into_parts(self) -> (Payload<'a>, usize, bool) {
        (self.payload, self.consumed, self.close_after)
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::Vectored;

    #[test]
    fn stable_source_adds_no_vectored_storage() {
        assert_eq!(size_of::<Vectored<'static>>(), 5 * size_of::<usize>());
    }
}
