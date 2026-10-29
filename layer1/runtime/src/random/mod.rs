//! Runtime-owned randomness bound to one exact generative driver domain.

use std::{hash, marker, mem};

use dope_core::{driver, driver::schedule::sequence, random::Seed};
use o3::base::marker::ThreadBound;

mod sip;

pub(crate) use sip::Sip;

type SipHasher13 = Sip<1, 3>;
type SipHasher24 = Sip<2, 4>;

/// Owned hashing state that remains confined to its creating thread.
pub type OwnedHasher = Sip<1, 3>;

pub(crate) fn derive_seed(seed: &Seed, domain: Domain) -> Seed {
    seed.map(|words| Key::new(words).derive(domain, ()).into_words())
}

type Invariant<'d> = marker::PhantomData<(fn(&'d ()) -> &'d (), ThreadBound)>;

/// Owned, thread-bound hash key derived from already acquired seed material.
/// Clones retain the same hash namespace without acquiring OS entropy or a
/// driver capability.
#[derive(Clone)]
pub struct HashSeed {
    state: (Key, ThreadBound),
}

impl HashSeed {
    pub fn from_seed(seed: &Seed, domain: Domain) -> Self {
        Self {
            state: (
                Key::new(*seed.as_ref()).derive(domain, ()),
                ThreadBound::NEW,
            ),
        }
    }

    pub fn hash<T: hash::Hash + ?Sized>(&self, input: &T) -> u64 {
        let mut state = self.state.0.hasher();
        input.hash(&mut state);
        hash::Hasher::finish(&state)
    }

    /// Hashes one complete byte string without `Hash` framing.
    pub fn hash_bytes(&self, input: &[u8]) -> u64 {
        self.state.0.hasher().hash(input)
    }
}

impl hash::BuildHasher for HashSeed {
    type Hasher = OwnedHasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.state.0.hasher()
    }
}

/// Keyed hash builder issued for one exact generative driver domain.
///
/// The driver lifetime is invariant and cannot be widened. Building a hasher
/// preserves the same invariant driver lifetime.
pub struct HashState<'d> {
    state: (Key, Invariant<'d>),
}

impl<'d> HashState<'d> {
    pub(crate) fn for_seed(seed: &Seed, domain: Domain) -> Self {
        Self {
            state: (
                Key::new(*seed.as_ref()).derive(domain, ()),
                marker::PhantomData,
            ),
        }
    }
}

impl<'d> hash::BuildHasher for HashState<'d> {
    type Hasher = Hasher<'d>;

    fn build_hasher(&self) -> Self::Hasher {
        Hasher::new(self.state.0.hasher(), self.state.1)
    }
}

/// Hashing state retaining the exact driver brand of its key source.
pub struct Hasher<'d> {
    state: (SipHasher13, Invariant<'d>),
}

impl<'d> Hasher<'d> {
    const fn new(inner: SipHasher13, proof: Invariant<'d>) -> Self {
        Self {
            state: (inner, proof),
        }
    }
}

impl hash::Hasher for Hasher<'_> {
    fn finish(&self) -> u64 {
        self.state.0.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.state.0.write(bytes);
    }

    fn write_u8(&mut self, value: u8) {
        self.state.0.write_u8(value);
    }

    fn write_u16(&mut self, value: u16) {
        self.state.0.write_u16(value);
    }

    fn write_u32(&mut self, value: u32) {
        self.state.0.write_u32(value);
    }

    fn write_u64(&mut self, value: u64) {
        self.state.0.write_u64(value);
    }

    fn write_usize(&mut self, value: usize) {
        self.state.0.write_usize(value);
    }
}

/// Compile-time purpose separating random roots without runtime state.
/// Implementations use a stable, globally distinct domain.
pub trait Purpose {
    const DOMAIN: Domain;
}

/// Keyed words and permutations supplied by a purpose-bound random owner.
/// Word generation and permutation rounds occupy separate domains even when
/// their compile-time lane is the same.
pub trait Entropy<P: Purpose> {
    fn word<const LANE: u64>(&self, input: u64) -> Word<P, LANE>;
    fn permute<const LANE: u64>(&self, value: u64) -> Word<P, LANE>;

    /// Derives an independent owned hash key, without exporting the root or its
    /// driver capability. Hash keys occupy a separate domain from public words
    /// and permutations, including when their compile-time lane is the same.
    fn hash_seed<const LANE: u64>(&self) -> HashSeed;
}

type RootState<'d, P> = (Key, Invariant<'d>, marker::PhantomData<fn(P) -> P>);
type ProcessState<P> = (Key, marker::PhantomData<(fn(P) -> P, ThreadBound)>);

/// Non-cloneable secret root bound invariantly to one driver domain.
/// Protocol authorities retain it for that lifetime and use compile-time lanes
/// to separate streams; storage is exactly the two secret key words.
pub struct Root<'d, P: Purpose> {
    state: RootState<'d, P>,
}

impl<'d, P: Purpose> Root<'d, P> {
    const fn new(seed: Key) -> Self {
        Self {
            state: (seed, marker::PhantomData, marker::PhantomData),
        }
    }

    /// Hashes a typed identity in a compile-time lane without materializing an
    /// intermediate byte representation.
    pub fn hash<const LANE: u64, T: hash::Hash + ?Sized>(&self, input: &T) -> Word<P, LANE> {
        self.state.0.hash::<P, LANE, T>(input)
    }
}

/// Non-cloneable process-owned secret root without a fabricated owner lifetime.
pub struct Process<P: Purpose> {
    state: ProcessState<P>,
}

impl<P: Purpose> Process<P> {
    pub fn random() -> Result<Self, dope_core::Errno> {
        let seed = Key::new(*Seed::acquire()?.as_ref()).derive(P::DOMAIN, ());
        Ok(Self {
            state: (seed, marker::PhantomData),
        })
    }
}

impl<P: Purpose> Entropy<P> for Root<'_, P> {
    fn word<const LANE: u64>(&self, input: u64) -> Word<P, LANE> {
        self.state.0.hash::<P, LANE, _>(&input)
    }

    fn permute<const LANE: u64>(&self, value: u64) -> Word<P, LANE> {
        self.state.0.permute::<P, LANE>(value)
    }

    fn hash_seed<const LANE: u64>(&self) -> HashSeed {
        self.state.0.hash_seed::<LANE>()
    }
}

impl<P: Purpose> Entropy<P> for Process<P> {
    fn word<const LANE: u64>(&self, input: u64) -> Word<P, LANE> {
        self.state.0.hash::<P, LANE, _>(&input)
    }

    fn permute<const LANE: u64>(&self, value: u64) -> Word<P, LANE> {
        self.state.0.permute::<P, LANE>(value)
    }

    fn hash_seed<const LANE: u64>(&self) -> HashSeed {
        self.state.0.hash_seed::<LANE>()
    }
}

impl<'d, P: Purpose> Root<'d, P> {
    /// Consumes a fresh identity from the driver before deriving a purpose root.
    /// Repeated acquisition never reuses the same derivation input.
    pub fn try_acquire(driver: &driver::Context<'_, 'd>) -> Result<Self, sequence::Exhausted> {
        let driver = driver.driver_ref();
        let instance = driver.try_next_id()?;
        let seed = Key::new(*driver.random_seed().as_ref());
        Ok(Self::new(seed.derive(P::DOMAIN, instance)))
    }
}

const _: () = assert!(mem::size_of::<HashState<'static>>() == 2 * mem::size_of::<u64>());
const _: () = assert!(mem::size_of::<HashSeed>() == 2 * mem::size_of::<u64>());
const _: () = assert!(
    mem::size_of::<OwnedHasher>() == 2 * mem::size_of::<usize>() + 5 * mem::size_of::<u64>()
);
const _: () = assert!(mem::size_of::<Hasher<'static>>() == mem::size_of::<SipHasher13>());

/// Secret key retained by one runtime authority.
#[repr(transparent)]
#[derive(Clone)]
struct Key([u64; 2]);

impl Key {
    const fn new(words: [u64; 2]) -> Self {
        Self(words)
    }

    const fn into_words(self) -> [u64; 2] {
        self.0
    }

    fn derive(&self, domain: Domain, instance: impl hash::Hash) -> Self {
        let mut state = self.prefix(domain);
        instance.hash(&mut state);
        Self::from_hasher(state)
    }

    fn from_hasher(state: SipHasher24) -> Self {
        Self([0, 1].map(|word| {
            let mut state = state;
            hash::Hasher::write_u64(&mut state, word);
            hash::Hasher::finish(&state)
        }))
    }

    fn hasher(&self) -> SipHasher13 {
        SipHasher13::new(self.0[0], self.0[1])
    }

    fn hash_seed<const LANE: u64>(&self) -> HashSeed {
        HashSeed {
            state: (Self::from_hasher(self.lane::<LANE>(2)), ThreadBound::NEW),
        }
    }

    fn hash<P, const LANE: u64, T: hash::Hash + ?Sized>(&self, input: &T) -> Word<P, LANE> {
        let mut state = self.lane::<LANE>(0);
        input.hash(&mut state);
        Word::new(hash::Hasher::finish(&state))
    }

    fn permute<P, const LANE: u64>(&self, value: u64) -> Word<P, LANE> {
        let state = self.lane::<LANE>(1);
        let mut left = (value >> 32) as u32;
        let mut right = value as u32;
        let mut round = 0;
        while round < 4 {
            let input = ((round as u64) << 32) | u64::from(right);
            let mut hash = state;
            hash::Hasher::write_u64(&mut hash, input);
            let next = left ^ hash::Hasher::finish(&hash) as u32;
            left = right;
            right = next;
            round += 1;
        }
        Word::new((u64::from(left) << 32) | u64::from(right))
    }

    fn lane<const LANE: u64>(&self, kind: u8) -> SipHasher24 {
        let mut state = SipHasher24::new(self.0[0], self.0[1]);
        hash::Hasher::write_u64(&mut state, LANE);
        hash::Hasher::write_u8(&mut state, kind);
        state
    }

    fn prefix(&self, domain: Domain) -> SipHasher24 {
        let mut state = SipHasher24::new(self.0[0], self.0[1]);
        hash::Hasher::write(&mut state, &domain.namespace);
        hash::Hasher::write_u64(&mut state, domain.instance);
        state
    }
}

/// Stable, human-readable ownership of one independent random namespace.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Namespace([u8; 8]);

impl Namespace {
    pub const fn new(name: [u8; 8]) -> Self {
        Self(name)
    }

    pub const fn fixed(self) -> Domain {
        self.instance(0)
    }

    pub const fn instance(self, instance: u64) -> Domain {
        Domain {
            namespace: self.0,
            instance,
        }
    }
}

/// Stable separation between one owned namespace and one exact instance.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Domain {
    namespace: [u8; 8],
    instance: u64,
}

/// One purpose- and lane-bound random word.
#[repr(transparent)]
pub struct Word<P, const LANE: u64>(u64, marker::PhantomData<fn(P) -> P>);

impl<P, const LANE: u64> Word<P, LANE> {
    const fn new(value: u64) -> Self {
        Self(value, marker::PhantomData)
    }

    /// Consumes this purpose-bound word into its canonical big-endian encoding.
    pub const fn into_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

const _: () = {
    assert!(mem::size_of::<Key>() == 2 * mem::size_of::<u64>());
    assert!(mem::size_of::<Namespace>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Domain>() == 2 * mem::size_of::<u64>());
    assert!(mem::size_of::<Word<(), 0>>() == mem::size_of::<u64>());
};
