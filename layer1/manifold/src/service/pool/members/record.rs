use o3::depot::queue::round::set;

use crate::service::{
    health::{Failures, Generation, Next},
    pool::{
        MemberKey, Target,
        members::{Occupied, Unlinked, Update, backoff, failure},
    },
};

/// Native schedule membership selects the live view of this canonical health value.
pub(super) struct Health {
    pub(super) generation: Generation,
    pub(super) failures: Failures,
    pub(super) idle: Idle,
    pub(super) ready: Ready,
    /// Desired members retain scheduling hints; retired members have no demand.
    /// Dial admission and retirement check the current source under its region token.
    pub(super) demand: Option<Demand>,
    pub(super) owners: u32,
}

#[derive(Debug)]
pub(super) struct Exhausted;

impl Health {
    const fn new() -> Self {
        Self {
            generation: Generation::INITIAL,
            failures: Failures::new(),
            idle: Idle::Exhausted,
            ready: Ready::Healthy,
            demand: Some(Demand::Both),
            owners: 0,
        }
    }
    pub(super) const fn target<'h>(&self, member: MemberKey<'h>) -> Target<'h> {
        Target {
            member,
            health: self.generation,
        }
    }

    pub(super) fn matches(&self, target: Target<'_>) -> bool {
        self.generation == target.health
    }

    pub(super) const fn try_ready(&mut self, ready: Ready) -> Result<(), Exhausted> {
        match self.generation.next() {
            Next::Advanced(epoch) => {
                self.generation = epoch;
                self.idle = Idle::Exhausted;
                if let Ready::Healthy = ready {
                    self.failures = Failures::new();
                }
                self.ready = ready;
                Ok(())
            }
            Next::Exhausted => {
                self.idle = Idle::Exhausted;
                Err(Exhausted)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::service::pool) enum Demand {
    Absent,
    Eligible,
    Retained,
    Both,
}

impl Demand {
    pub(in crate::service::pool) const fn new(eligible: bool, retained: bool) -> Self {
        match (eligible, retained) {
            (false, false) => Self::Absent,
            (true, false) => Self::Eligible,
            (false, true) => Self::Retained,
            (true, true) => Self::Both,
        }
    }

    pub(in crate::service::pool) const fn is_retained(self) -> bool {
        matches!(self, Self::Retained | Self::Both)
    }
}

#[derive(PartialEq, Eq)]
pub(super) enum Ready {
    Healthy,
    Probe,
}

/// Only unscheduled residents use this state; the order owns scheduled eligibility.
pub(super) enum Idle {
    Backoff,
    Probing,
    Exhausted,
}

pub(super) struct Record<I, A> {
    pub(super) id: I,
    pub(super) addr: A,
    pub(super) health: Health,
}

impl<I, A> Record<I, A> {
    pub(super) fn new(id: I, addr: A) -> Self {
        Self {
            id,
            addr,
            health: Health::new(),
        }
    }

    pub(super) fn is_desired(&self) -> bool {
        self.health.demand.is_some()
    }

    /// Only successful health advancement can consume idle ownership into schedule membership.
    pub(super) fn try_resume<'a>(
        mut entry: Unlinked<'a, '_, I, A>,
        ready: Ready,
    ) -> Result<&'a mut Self, Exhausted> {
        entry.get_mut().health.try_ready(ready)?;
        Ok(entry.insert().into_mut())
    }

    pub(super) fn cancel<'h>(entry: Occupied<'_, 'h, I, A>, target: Target<'h>) -> Update {
        if !entry.get().health.matches(target) {
            return Update::Ignored;
        }
        let set::State::Idle(entry) = entry.state() else {
            return Update::Ignored;
        };
        if !matches!(entry.get().health.idle, Idle::Probing) {
            return Update::Ignored;
        }
        match Self::try_resume(entry, Ready::Probe) {
            Ok(_) => Update::Applied,
            Err(self::Exhausted) => Update::Exhausted,
        }
    }
}

const _: () = {
    assert!(size_of::<Health>() == size_of::<(Generation, [u8; 4])>());
    assert!(size_of::<Ready>() == 1);
    assert!(size_of::<Idle>() == 1);
    assert!(size_of::<Option<Demand>>() == size_of::<bool>());
    assert!(size_of::<Record<u32, u32>>() == size_of::<Health>() + size_of::<[u32; 2]>());
    assert!(
        size_of::<backoff::Backoff<'static, 'static, u32, u32, true>>()
            == size_of::<Unlinked<'static, 'static, u32, u32>>()
    );
    assert!(
        size_of::<failure::Failure<'static, 'static, u32, u32>>()
            == size_of::<Unlinked<'static, 'static, u32, u32>>()
    );
};
