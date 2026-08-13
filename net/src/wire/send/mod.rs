use std::{marker, ops};

use dope_core::io::socket::msg;
use o3::buffer::view::window;

use crate::wire::reclaim;

#[doc(hidden)]
pub mod raw;

pub(crate) enum Outcome<P: reclaim::Policy> {
    Submitted(usize, marker::PhantomData<fn() -> P>),
    Rejected(usize, marker::PhantomData<fn() -> P>),
    Idle(usize, marker::PhantomData<fn() -> P>),
}

impl<P: reclaim::Policy> Clone for Outcome<P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: reclaim::Policy> Copy for Outcome<P> {}

impl<P: reclaim::Policy> Outcome<P> {
    pub(crate) fn submitted(consumed: usize) -> Self {
        Self::Submitted(consumed, marker::PhantomData)
    }

    pub(crate) fn rejected(consumed: usize) -> Self {
        Self::Rejected(consumed, marker::PhantomData)
    }

    pub(crate) fn idle(consumed: usize) -> Self {
        Self::Idle(consumed, marker::PhantomData)
    }
}

const _: () =
    assert!(std::mem::size_of::<Outcome<reclaim::OnSubmit>>() == 2 * std::mem::size_of::<usize>());
const _: () = assert!(
    std::mem::size_of::<Outcome<reclaim::OnSubmit>>()
        == std::mem::size_of::<Outcome<reclaim::OnComplete>>()
);

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

/// Inline connection-local send storage.
/// Its fixed slab owner keeps the address stable during retained sends.
#[repr(transparent)]
pub struct Buffer<const CAP: usize> {
    buf: window::Inline<CAP>,
}

impl<const CAP: usize> Buffer<CAP> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: window::Inline::default(),
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

    pub fn try_extend(&mut self, src: &[u8]) -> bool {
        self.buf.try_extend(src).is_ok()
    }

    pub fn try_consume(&mut self, n: usize) -> bool {
        use o3::buffer::PrefixConsumer;

        let Ok(prefix) = PrefixConsumer::try_consume_prefix(&mut self.buf, n) else {
            return false;
        };
        prefix.commit();
        true
    }
}

impl<const CAP: usize> Default for Buffer<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

/// Send storage borrowed exclusively while a prepared send retains its bytes.
pub trait StorageBackend {
    fn as_slice(&self) -> &[u8];

    /// Consumes the backend and reports whether that returned shared capacity.
    fn release(self) -> Availability;
}

impl<const CAP: usize> StorageBackend for Buffer<CAP> {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }

    fn release(self) -> Availability {
        Availability::Unchanged
    }
}

impl StorageBackend for () {
    fn as_slice(&self) -> &[u8] {
        &[]
    }

    fn release(self) -> Availability {
        Availability::Unchanged
    }
}

pub struct Storage<'a, S: StorageBackend> {
    storage: &'a mut S,
    limit: usize,
}

impl<'a, S: StorageBackend> Storage<'a, S> {
    pub(crate) fn new(storage: &'a mut S, limit: usize) -> Self {
        Self { storage, limit }
    }

    /// Constructs the exclusive storage view used by raw wire harnesses.
    #[doc(hidden)]
    pub fn from_raw(storage: &'a mut S, limit: usize) -> Self {
        Self::new(storage, limit)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.storage.as_slice()
    }

    /// Prepares connection-owned output after consuming caller input.
    /// The output no longer borrows the input, so reclamation completes at
    /// submission; `OnComplete` must retain its exact input instead.
    pub fn buffered(self, consumed: usize) -> Prepared<'a, reclaim::OnSubmit> {
        let consumed = consumed.min(self.limit);
        let bytes = self.storage.as_slice();
        if bytes.is_empty() {
            Prepared {
                payload: Payload::Empty,
                consumed,
                flags: 0,
                _policy: marker::PhantomData,
            }
        } else {
            Prepared {
                payload: Payload::Single(Plain::proven(bytes)),
                consumed,
                flags: 0,
                _policy: marker::PhantomData,
            }
        }
    }

    /// Prepares no output and consumes no caller input.
    pub fn empty<P: reclaim::Policy>(self) -> Prepared<'a, P> {
        Prepared {
            payload: Payload::Empty,
            consumed: 0,
            flags: 0,
            _policy: marker::PhantomData,
        }
    }

    /// Consumes caller input without retaining or emitting output.
    pub fn consume(self, consumed: usize) -> Prepared<'a, reclaim::OnSubmit> {
        Prepared {
            payload: Payload::Empty,
            consumed: consumed.min(self.limit),
            flags: 0,
            _policy: marker::PhantomData,
        }
    }

    /// Prepares independent process-static output.
    pub fn static_slice(self, buf: &'static [u8]) -> Prepared<'a, reclaim::OnSubmit> {
        Prepared {
            payload: Payload::Single(Plain::proven(buf)),
            consumed: 0,
            flags: 0,
            _policy: marker::PhantomData,
        }
    }
}

impl<S: StorageBackend> ops::Deref for Storage<'_, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.storage
    }
}

impl<S: StorageBackend> ops::DerefMut for Storage<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.storage
    }
}

pub struct Plain<'a> {
    bytes: &'a [u8],
}

impl<'a> Plain<'a> {
    fn proven(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Constructs a direct-send view whose storage is live for the process.
    pub const fn from_static(bytes: &'static [u8]) -> Plain<'static> {
        Plain { bytes }
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
    message: msg::Vectored<'a>,
}

impl<'a> Vectored<'a> {
    #[doc(hidden)]
    pub fn from_message(message: msg::Vectored<'a>) -> Self {
        Self { message }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        self.message.iter()
    }

    #[must_use]
    /// Returns whether the descriptors contain no payload bytes.
    ///
    /// This is true even when the source contains zero-length descriptors.
    pub fn is_empty(&self) -> bool {
        self.message.is_empty()
    }

    #[must_use]
    /// Returns the exact payload length cached by the retained source.
    pub fn bytes(&self) -> usize {
        self.message.bytes()
    }

    pub(crate) fn message(&self) -> msg::Message<'a> {
        self.message.message()
    }
}

pub(crate) enum Payload<'a> {
    Empty,
    Single(Plain<'a>),
    Vectored(Vectored<'a>),
}

/// Whether an operation returned shared wire send storage.
#[must_use]
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    /// No shared send storage was returned.
    Unchanged = 0,
    /// Shared send storage was returned.
    Released = 1 << 1,
}

impl Availability {
    #[must_use]
    pub const fn is_released(self) -> bool {
        matches!(self, Self::Released)
    }
}

const CLOSE_AFTER: u8 = 1 << 0;
const RELEASED: u8 = 1 << 1;
const _: () = assert!(Availability::Released as u8 == RELEASED);

#[must_use]
pub struct Prepared<'a, P: reclaim::Policy> {
    payload: Payload<'a>,
    consumed: usize,
    flags: u8,
    _policy: marker::PhantomData<P>,
}

#[must_use]
#[repr(transparent)]
/// Resource transition caused by one send completion.
/// `OnSubmit` may carry independent follow-up output; `OnComplete` can only
/// terminate the exact retained input represented by that completion.
pub struct Transition<'a, P: reclaim::Policy>(Prepared<'a, P>);

impl<'a> Transition<'a, reclaim::OnSubmit> {
    pub fn new(mut prepared: Prepared<'a, reclaim::OnSubmit>, availability: Availability) -> Self {
        prepared.flags |= availability as u8;
        Self(prepared)
    }

    pub fn unchanged(prepared: Prepared<'a, reclaim::OnSubmit>) -> Self {
        Self::new(prepared, Availability::Unchanged)
    }
}

impl<'a> Transition<'a, reclaim::OnComplete> {
    /// Completes one exact retained input without chaining unrelated output.
    pub fn completed<S: StorageBackend>(send: Storage<'a, S>) -> Self {
        Self(send.empty())
    }
}

impl<'a, P: reclaim::Policy> Transition<'a, P> {
    pub const fn availability(&self) -> Availability {
        if self.0.flags & RELEASED == 0 {
            Availability::Unchanged
        } else {
            Availability::Released
        }
    }

    pub(crate) fn into_parts(self) -> (Prepared<'a, P>, Availability) {
        let availability = self.availability();
        (self.0, availability)
    }

    #[doc(hidden)]
    pub fn inspect(self) -> (bool, usize, bool, Availability) {
        let (prepared, availability) = self.into_parts();
        let (empty, consumed, close_after) = prepared.inspect();
        (empty, consumed, close_after, availability)
    }
}

impl<'a> Prepared<'a, reclaim::OnComplete> {
    /// Retains exact owner-backed input bytes through terminal completion.
    pub fn input(plain: Plain<'a>) -> Self {
        let consumed = plain.len();
        Self {
            payload: Payload::Single(plain),
            consumed,
            flags: 0,
            _policy: marker::PhantomData,
        }
    }

    /// Retains exact owner-backed input bytes and descriptor storage through
    /// terminal completion.
    pub fn vectored(plain: Vectored<'a>) -> Self {
        let consumed = plain.bytes();
        Self {
            payload: Payload::Vectored(plain),
            consumed,
            flags: 0,
            _policy: marker::PhantomData,
        }
    }
}

impl<'a, P: reclaim::Policy> Prepared<'a, P> {
    pub fn close_after(mut self) -> Self {
        self.flags |= CLOSE_AFTER;
        self
    }

    pub(crate) fn into_parts(self) -> (Payload<'a>, usize, bool) {
        (self.payload, self.consumed, self.flags & CLOSE_AFTER != 0)
    }

    #[doc(hidden)]
    pub fn inspect(self) -> (bool, usize, bool) {
        let (payload, consumed, close_after) = self.into_parts();
        (matches!(payload, Payload::Empty), consumed, close_after)
    }
}

const _: () = assert!(
    std::mem::size_of::<Prepared<'static, reclaim::OnSubmit>>()
        == std::mem::size_of::<Prepared<'static, reclaim::OnComplete>>()
);
const _: () = assert!(
    std::mem::align_of::<Prepared<'static, reclaim::OnSubmit>>()
        == std::mem::align_of::<Prepared<'static, reclaim::OnComplete>>()
);
const _: () = assert!(
    std::mem::size_of::<Transition<'static, reclaim::OnSubmit>>()
        == std::mem::size_of::<Prepared<'static, reclaim::OnSubmit>>()
);
