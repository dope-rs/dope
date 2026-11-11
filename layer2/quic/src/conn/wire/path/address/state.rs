use std::{mem, num, time};

use o3::depot::{array::copy, capacity};

use crate::conn::wire::path;
use crate::conn::wire::path::address;
use crate::proto::path::Peer;
use crate::wire::packet::header;
use crate::wire::stream::Initiator;

use address::policy;
use path::Address;

#[derive(Clone, Copy)]
pub(in crate::conn::wire::path) struct Ingress {
    pub(in crate::conn::wire::path) address: Peer,
    pub(in crate::conn::wire::path) bytes: usize,
    pub(in crate::conn::wire::path) outer_bytes: usize,
    pub(in crate::conn::wire::path) challenge: Option<[[u8; 8]; 2]>,
}

#[derive(Clone, Copy)]
pub(in crate::conn::wire::path) struct Candidate {
    path: path::Key,
    deadline: time::Instant,
    attempt: Attempt,
    proof: Proof,
}

#[derive(Clone, Copy)]
enum Attempt {
    Expanded,
    First,
    Second,
    Last,
    Stopped,
}

#[derive(Clone, Copy)]
enum Proof {
    None,
    Reachable,
    NonProbing,
    Complete,
}

impl Attempt {
    const fn can_retry(self) -> bool {
        matches!(self, Self::Expanded | Self::First | Self::Second)
    }

    const fn left(self) -> u32 {
        match self {
            Self::Expanded => 3,
            Self::First => 2,
            Self::Second => 1,
            Self::Last | Self::Stopped => 0,
        }
    }

    fn advance(self) -> Option<Self> {
        match self {
            Self::Expanded => Some(Self::First),
            Self::First => Some(Self::Second),
            Self::Second => Some(Self::Last),
            Self::Last | Self::Stopped => None,
        }
    }
}

impl Proof {
    const fn is_validated(self) -> bool {
        matches!(self, Self::Reachable | Self::Complete)
    }

    const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }

    const fn validate(self) -> Self {
        match self {
            Self::None | Self::Reachable => Self::Reachable,
            Self::NonProbing | Self::Complete => Self::Complete,
        }
    }

    const fn observe(self, active: bool) -> Self {
        match (self.is_validated(), active) {
            (false, false) => Self::None,
            (false, true) => Self::NonProbing,
            (true, false) => Self::Reachable,
            (true, true) => Self::Complete,
        }
    }
}

impl Candidate {
    pub(in crate::conn::wire::path) fn new(path: path::Key, deadline: time::Instant) -> Self {
        Self {
            path,
            deadline,
            attempt: Attempt::First,
            proof: Proof::None,
        }
    }

    pub(in crate::conn::wire::path) const fn path(self) -> path::Key {
        self.path
    }

    pub(in crate::conn::wire::path) const fn deadline(self) -> Option<time::Instant> {
        if self.proof.is_validated() || self.is_stopped() {
            None
        } else {
            Some(self.deadline)
        }
    }

    pub(in crate::conn::wire::path) fn is_retry_due(self, now: time::Instant) -> bool {
        !self.proof.is_validated() && now >= self.deadline && self.attempt.can_retry()
    }

    pub(in crate::conn::wire::path) fn is_expired(
        self,
        now: time::Instant,
        pto: time::Duration,
    ) -> bool {
        let Some(expiry) = pto
            .checked_mul(self.attempt.left())
            .and_then(|delay| self.deadline.checked_add(delay))
        else {
            return false;
        };
        now >= expiry
    }

    pub(in crate::conn::wire::path) fn retry(&mut self, pto: time::Duration) -> Option<Retried> {
        if self.proof.is_validated() || !self.attempt.can_retry() {
            return None;
        }
        let deadline = self.deadline.checked_add(pto)?;
        let attempt = self.attempt.advance()?;
        self.deadline = deadline;
        self.attempt = attempt;
        Some(Retried)
    }

    pub(in crate::conn::wire::path) const fn validate(&mut self) {
        self.proof = self.proof.validate();
    }

    pub(in crate::conn::wire::path) fn expand(&mut self, now: time::Instant) {
        self.deadline = now;
        self.attempt = Attempt::Expanded;
    }

    pub(in crate::conn::wire::path) const fn observe(&mut self, active: bool) {
        self.proof = self.proof.observe(active);
    }

    pub(in crate::conn::wire::path) const fn is_complete(self) -> bool {
        self.proof.is_complete()
    }

    pub(in crate::conn::wire::path) const fn is_stopped(self) -> bool {
        matches!(self.attempt, Attempt::Stopped)
    }

    pub(in crate::conn::wire::path) fn stop(&mut self) {
        self.attempt = Attempt::Stopped;
    }
}

pub(in crate::conn::wire::path) struct Retried;

#[derive(Clone, Copy)]
pub(in crate::conn::wire::path) enum Validation {
    Candidate,
    Previous,
}

#[derive(Clone, Copy)]
pub(in crate::conn::wire::path) struct Observed {
    pub(in crate::conn::wire::path) key: path::Key,
    pub(in crate::conn::wire::path) address: Peer,
    pub(in crate::conn::wire::path) received: u64,
    pub(in crate::conn::wire::path) sent: u64,
    pub(in crate::conn::wire::path) outer_received: u64,
    pub(in crate::conn::wire::path) outer_sent: u64,
    pub(in crate::conn::wire::path) validated: bool,
}

pub(in crate::conn::wire::path) struct State {
    pub(in crate::conn::wire::path) active: Option<Peer>,
    pub(in crate::conn::wire::path) ingress: Option<Ingress>,
    pub(in crate::conn::wire::path) candidate: Option<Candidate>,
    pub(in crate::conn::wire::path) previous: Option<Candidate>,
    pub(in crate::conn::wire::path) highest_non_probing: Option<header::Number>,
    pub(in crate::conn::wire::path) observed: copy::Vec<Observed, 2>,
    next_path_key: num::NonZero<u64>,
    policy: policy::Policy,
}

impl State {
    pub(in crate::conn::wire::path) fn new(local: Initiator, fixed: bool) -> Self {
        Self {
            active: None,
            ingress: None,
            candidate: None,
            previous: None,
            highest_non_probing: None,
            observed: copy::Vec::new(),
            next_path_key: num::NonZero::<u64>::MIN,
            policy: policy::Policy::new(local, fixed),
        }
    }

    pub(in crate::conn::wire::path) fn allows(&self, active: Peer, source: Peer) -> bool {
        self.policy.allows(active, source)
    }

    pub(in crate::conn::wire::path) fn next_validation(&self) -> Option<(Validation, Candidate)> {
        self.candidate
            .map(|item| (Validation::Candidate, item))
            .into_iter()
            .chain(self.previous.map(|item| (Validation::Previous, item)))
            .filter_map(|(kind, item)| item.deadline().map(|deadline| (kind, item, deadline)))
            .min_by_key(|(_, _, deadline)| *deadline)
            .map(|(kind, item, _)| (kind, item))
    }

    pub(in crate::conn::wire::path) fn key_for(&self, address: Peer) -> Option<path::Key> {
        self.observed
            .iter()
            .find(|path| path.address == address)
            .map(|path| path.key)
    }

    pub(in crate::conn::wire::path) fn get(&self, key: path::Key) -> Option<&Observed> {
        self.observed.iter().find(|path| path.key == key)
    }

    pub(in crate::conn::wire::path) fn get_mut(&mut self, key: path::Key) -> Option<&mut Observed> {
        self.observed.iter_mut().find(|path| path.key == key)
    }

    pub(in crate::conn::wire::path) fn try_observe(
        &mut self,
        address: Peer,
        bytes: usize,
        outer_bytes: usize,
    ) -> Result<path::Key, Address> {
        if let Some(path) = self
            .observed
            .iter_mut()
            .find(|path| path.address == address)
        {
            let Some(received) = path.received.checked_add(bytes as u64) else {
                return Err(Address::Bytes {
                    address,
                    received: path.received,
                    bytes,
                });
            };
            let Some(outer_received) = path.outer_received.checked_add(outer_bytes as u64) else {
                return Err(Address::OuterBytes {
                    address,
                    received: path.outer_received,
                    bytes: outer_bytes,
                });
            };
            path.received = received;
            path.outer_received = outer_received;
            return Ok(path.key);
        }
        let vacant = self.observed.vacancy().ok_or(Address::Capacity {
            address,
            reason: capacity::Reason::Full,
        })?;
        let next = self.next_path_key;
        let Some(after) = next.checked_add(1) else {
            return Err(Address::Keys {
                address,
                next: next.get(),
            });
        };
        let key = path::Key(next);
        self.next_path_key = after;
        vacant.insert(Observed {
            key,
            address,
            received: bytes as u64,
            sent: 0,
            outer_received: outer_bytes as u64,
            outer_sent: 0,
            validated: Some(address) == self.active,
        });
        Ok(key)
    }

    pub(in crate::conn::wire::path) fn remove(&mut self, key: path::Key) {
        self.observed.retain(|path| path.key != key);
    }
}

const _: () = {
    assert!(mem::size_of::<Attempt>() == 1);
    assert!(mem::size_of::<Proof>() == 1);
    assert!(mem::size_of::<Candidate>() == 32);
    assert!(!mem::needs_drop::<Candidate>());
    assert!(
        mem::size_of::<copy::Vec<Observed, 2>>()
            == 2 * mem::size_of::<Observed>() + mem::size_of::<u64>()
    );
};
