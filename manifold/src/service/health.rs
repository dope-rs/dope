use std::{hash::BuildHasher as _, io, time};

use dope_runtime::random;

const MAX_SHIFT: u8 = 10;

const HASH_DOMAIN_BASE: u64 = u64::from_be_bytes(*b"\0backoff");

/// Health-backoff hash domains separated by connector identity.
pub enum Domain {}

impl Domain {
    /// Health-backoff hash domain for connector zero.
    pub const DEFAULT: random::Domain = Self::for_connector(0);

    pub const fn for_connector(connector_id: u8) -> random::Domain {
        random::Domain::new(HASH_DOMAIN_BASE ^ connector_id as u64)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Connect,
    Transport,
    Idle,
    Protocol,
    Remote,
}

pub struct Backoff {
    base: time::Duration,
    rng: u64,
}

#[repr(transparent)]
pub(crate) struct Failures(u8);

impl Failures {
    pub(crate) const fn new() -> Self {
        Self(0)
    }

    pub(crate) fn reset(&mut self) {
        self.0 = 0;
    }

    fn increment(&mut self) -> u8 {
        if self.0 < MAX_SHIFT {
            self.0 += 1;
        }
        self.0
    }
}

impl Backoff {
    pub fn new(base: time::Duration, hash_builder: random::HashState<'_>) -> io::Result<Self> {
        let max_multiplier = 1u32 << u32::from(MAX_SHIFT);
        let max_delay = base
            .checked_mul(max_multiplier)
            .ok_or_else(|| Self::invalid("service backoff exceeds the duration range"))?;
        if base.is_zero() {
            return Err(Self::invalid("service backoff must be non-zero"));
        }
        if time::Instant::now().checked_add(max_delay).is_none() {
            return Err(Self::invalid(
                "service backoff exceeds the monotonic clock range",
            ));
        }
        let rng = hash_builder.hash_one(base.as_nanos()).max(1);
        Ok(Self { base, rng })
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn deadline(&mut self, now: time::Instant, attempt: u8) -> time::Instant {
        let scaled = self
            .base
            .saturating_mul(1u32 << u32::from(attempt.min(MAX_SHIFT)));
        let half = scaled / 2;
        let random = self.next_rand();
        let jitter_ns = match u64::try_from(half.as_nanos()) {
            Ok(0) => 0,
            Ok(span) => random % span,
            Err(_) => random,
        };
        let jitter = time::Duration::from_nanos(jitter_ns);
        let delay = half.saturating_add(jitter);
        match now.checked_add(delay) {
            Some(deadline) => deadline,
            Option::None => Self::furthest_deadline(now, delay),
        }
    }

    #[cold]
    fn furthest_deadline(now: time::Instant, requested: time::Duration) -> time::Instant {
        const RESOLUTION: time::Duration = time::Duration::from_nanos(1);

        let mut lower = time::Duration::ZERO;
        let mut upper = requested;
        let mut deadline = now;
        while lower < upper {
            let distance = upper - lower;
            let half = distance / 2;
            let candidate = if half.is_zero() {
                upper
            } else {
                lower.saturating_add(half)
            };
            match now.checked_add(candidate) {
                Some(value) => {
                    lower = candidate;
                    deadline = value;
                }
                Option::None => upper = candidate.saturating_sub(RESOLUTION),
            }
        }
        deadline
    }

    fn invalid(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, message)
    }

    pub(crate) fn retry_at(
        &mut self,
        failures: &mut Failures,
        now: time::Instant,
    ) -> time::Instant {
        self.deadline(now, failures.increment())
    }
}
