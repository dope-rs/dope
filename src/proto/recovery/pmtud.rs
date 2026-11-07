use std::{mem, num};

use crate::proto::datagram;

pub(crate) const BASE_PMTU: u64 = 1200;
pub(crate) const DEFAULT_MAX_PMTU: u64 = 1500;
pub(crate) const MAX_PMTU: u64 = 65_527;
const MAX_PROBES_PER_SIZE: u8 = 3;
const SEARCH_DONE_INTERVAL: u64 = 4;

#[derive(Clone, Copy)]
#[repr(u8)]
enum Failures {
    None,
    One,
    Two,
}

impl Failures {
    const fn next(self) -> (Self, bool) {
        match self {
            Self::None => (Self::One, false),
            Self::One => (Self::Two, false),
            Self::Two => (Self::None, true),
        }
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Probe(num::NonZero<u16>);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Maximum(datagram::Size);

impl Maximum {
    pub(crate) const fn new(value: u64) -> Option<Self> {
        if value > MAX_PMTU {
            return None;
        }
        match datagram::Size::new(value as u32) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub(crate) const fn cap(self, value: datagram::Size) -> Self {
        if value.get() < self.0.get() {
            Self(value)
        } else {
            self
        }
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get() as u64
    }
}

impl Probe {
    pub(crate) const fn get(self) -> usize {
        self.0.get() as usize
    }
}

pub(crate) struct Pmtud {
    upper_bound: u64,
    failures: Failures,
}

impl Pmtud {
    pub(crate) fn new(max_pmtu: Maximum) -> Self {
        Self {
            upper_bound: max_pmtu.get(),
            failures: Failures::None,
        }
    }

    /// Applies the authenticated peer UDP payload ceiling before discovery can
    /// arm a probe. Transport parameters guarantee a minimum of 1200 bytes.
    pub(crate) fn cap(&mut self, max: u64, current: u64) -> Option<Maximum> {
        if current != BASE_PMTU || !matches!(self.failures, Failures::None) {
            return None;
        }
        let maximum = Maximum::new(self.upper_bound.min(max))?;
        self.upper_bound = maximum.get();
        Some(maximum)
    }

    pub(crate) fn next_probe(&self, current: u64, ceiling: num::NonZero<u16>) -> Option<Probe> {
        let upper_bound = self.upper_bound.min(u64::from(ceiling.get()));
        if upper_bound <= current + SEARCH_DONE_INTERVAL {
            return None;
        }
        let size = (current + upper_bound) / 2;
        let size = num::NonZero::<u16>::new(u16::try_from(size).ok()?)?;
        Some(Probe(size))
    }

    pub(crate) fn probe_acked(&mut self) {
        self.failures = Failures::None;
    }

    pub(crate) fn probe_lost(&mut self, probe: Probe) {
        let (failures, done) = self.failures.next();
        self.failures = failures;
        if done {
            self.upper_bound = probe.get() as u64 - 1;
        }
    }
}

const _: () = assert!(MAX_PMTU <= u64::MAX - SEARCH_DONE_INTERVAL);
const _: () = assert!(mem::size_of::<Option<Probe>>() == mem::size_of::<u16>());
const _: () = assert!(mem::size_of::<Maximum>() == mem::size_of::<u16>());
const _: () = assert!(mem::size_of::<Option<Maximum>>() == mem::size_of::<u16>());
const _: () = assert!(mem::size_of::<Pmtud>() <= 16);
const _: () = assert!(MAX_PROBES_PER_SIZE == 3);
