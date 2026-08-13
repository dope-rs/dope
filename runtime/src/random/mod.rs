//! Runtime-owned randomness bound to one exact generative driver domain.

use std::{hash, marker, mem};

use siphasher::sip;

mod seed;

pub(crate) use seed::Seed;

/// Keyed hash builder issued for one exact generative driver domain.
///
/// The driver lifetime is invariant and cannot be widened:
///
/// ```compile_fail
/// use dope_runtime::random::HashState;
///
/// fn escape<'d>(state: HashState<'d>) -> HashState<'static> {
///     state
/// }
/// ```
///
/// Construction is reserved for a runtime session holding the matching brand:
///
/// ```compile_fail,E0624
/// use dope_runtime::random::HashState;
///
/// let _ = HashState::new([1, 2]);
/// ```
///
/// Building a hasher preserves the same invariant driver lifetime:
///
/// ```compile_fail
/// use std::hash::BuildHasher;
/// use dope_runtime::random::{HashState, Hasher};
///
/// fn erase<'d>(state: HashState<'d>) -> Hasher<'static> {
///     state.build_hasher()
/// }
/// ```
#[derive(Clone, Copy)]
pub struct HashState<'d> {
    words: [u64; 2],
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    _thread: o3::ThreadBound,
}

impl<'d> HashState<'d> {
    const fn new(words: [u64; 2]) -> Self {
        Self {
            words,
            _driver: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }
}

impl<'d> hash::BuildHasher for HashState<'d> {
    type Hasher = Hasher<'d>;

    fn build_hasher(&self) -> Self::Hasher {
        Hasher::new(sip::SipHasher13::new_with_keys(
            self.words[0],
            self.words[1],
        ))
    }
}

const _: () = assert!(mem::size_of::<HashState<'static>>() == 2 * mem::size_of::<u64>());

/// Hashing state retaining the exact driver brand of its key source.
#[doc(hidden)]
pub struct Hasher<'d> {
    inner: sip::SipHasher13,
    _driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    _thread: o3::ThreadBound,
}

impl<'d> Hasher<'d> {
    const fn new(inner: sip::SipHasher13) -> Self {
        Self {
            inner,
            _driver: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }
}

impl hash::Hasher for Hasher<'_> {
    fn finish(&self) -> u64 {
        self.inner.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.inner.write(bytes);
    }

    fn write_u8(&mut self, value: u8) {
        self.inner.write_u8(value);
    }

    fn write_u16(&mut self, value: u16) {
        self.inner.write_u16(value);
    }

    fn write_u32(&mut self, value: u32) {
        self.inner.write_u32(value);
    }

    fn write_u64(&mut self, value: u64) {
        self.inner.write_u64(value);
    }

    fn write_usize(&mut self, value: usize) {
        self.inner.write_usize(value);
    }
}

const _: () = assert!(mem::size_of::<Hasher<'static>>() == mem::size_of::<sip::SipHasher13>());

/// Stable separation between independent uses of one worker seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Domain(u64);

impl Domain {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn from_bytes(value: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(value))
    }

    pub(super) const fn get(self) -> u64 {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Domain>() == mem::size_of::<u64>());
