use std::{hash::BuildHasher as _, time};

use dope_core::driver::schedule::clock;
use dope_net::link::event;
use dope_runtime::random::{self, HashState};
use o3::base::time::Positive;

const HASH_NAMESPACE: random::Namespace = random::Namespace::new(*b"\0backoff");

/// Health-backoff hash domains separated by connector identity.
pub enum Domain {}

impl Domain {
    /// Health-backoff hash domain for connector zero.
    pub const DEFAULT: random::Domain = Self::for_id(0);

    pub const fn for_id(id: u8) -> random::Domain {
        HASH_NAMESPACE.instance(id as u64)
    }
}

pub enum Cause {
    Connect(event::ConnectError),
    ConnectTimeout,
    Transport,
    Idle,
    Protocol,
    Remote,
}

pub struct Backoff {
    plan: Plan,
    rng: u64,
}

/// Nonzero, ordered retry bounds admitted before runtime construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Plan {
    base: Positive,
    maximum: time::Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Base,
    Order {
        base: time::Duration,
        maximum: time::Duration,
    },
}

impl Plan {
    pub fn new(base: time::Duration, maximum: time::Duration) -> Result<Self, Error> {
        let base = Positive::try_new(base).map_err(|_| Error::Base)?;
        if maximum < base.get() {
            return Err(Error::Order {
                base: base.get(),
                maximum,
            });
        }
        Ok(Self { base, maximum })
    }

    pub fn bind(self, hash: HashState<'_>) -> Backoff {
        let rng = hash.hash_one((self.base.get().as_nanos(), self.maximum.as_nanos()));
        Backoff { plan: self, rng }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct Generation(u64);

pub(crate) enum Next {
    Advanced(Generation),
    Exhausted,
}

impl Generation {
    pub(crate) const INITIAL: Self = Self(0);
    pub(crate) const LAST: Self = Self(u64::MAX);

    pub(crate) const fn next(self) -> Next {
        if self.0 == Self::LAST.0 {
            Next::Exhausted
        } else {
            Next::Advanced(Self(self.0 + 1))
        }
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

#[repr(transparent)]
pub(crate) struct Failures(u8);

enum Retry {
    Scale(u32),
    Maximum,
}

impl Failures {
    pub(crate) const fn new() -> Self {
        Self(0)
    }

    fn advance(&mut self) -> Retry {
        if u32::from(self.0) >= u32::BITS {
            return Retry::Maximum;
        }
        let factor = 1_u32 << u32::from(self.0);
        self.0 += 1;
        Retry::Scale(factor)
    }
}

impl Backoff {
    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut x = self.rng;
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    /// Keeps the admitted positive minimum when halving one nanosecond rounds to zero.
    fn deadline<'d>(&mut self, now: clock::Moment<'d>, retry: Retry) -> clock::Deadline<'d> {
        let scaled = match retry {
            Retry::Scale(factor) => self
                .plan
                .base
                .get()
                .saturating_mul(factor)
                .min(self.plan.maximum),
            Retry::Maximum => self.plan.maximum,
        };
        let half = scaled / 2;
        let random = self.next_rand();
        let span = half.as_nanos();
        let nanos = if span == 0 {
            1
        } else if span > u128::from(u64::MAX) {
            random
        } else {
            random % span as u64
        };
        let jitter = time::Duration::from_nanos(nanos);
        let delay = half.saturating_add(jitter);
        now.clamp_add(delay)
    }

    pub(crate) fn retry_at<'d>(
        &mut self,
        failures: &mut Failures,
        now: clock::Moment<'d>,
    ) -> clock::Deadline<'d> {
        let retry = failures.advance();
        self.deadline(now, retry)
    }
}
