use dope_core::driver::schedule::clock;
use o3::depot::queue::round::set;

use crate::service::{
    health::{Failures, Generation, Next},
    pool::{
        MemberKey, Target,
        members::{Occupied, Unlinked, Update, backoff, failure, queues},
    },
};

/// Native schedule membership selects the live view of this canonical health value.
pub(super) struct Health {
    pub(super) generation: Generation,
    pub(super) failures: Failures,
    pub(super) idle: Idle,
    pub(super) ready: Ready,
    /// Desired members retain an eligibility hint; retired members have no demand.
    /// Every dial checks the current source under its region token.
    pub(super) demand: Option<bool>,
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
            demand: Some(true),
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
    assert!(size_of::<Option<bool>>() == size_of::<bool>());
    assert!(size_of::<Record<u32, u32>>() == size_of::<Health>() + size_of::<[u32; 2]>());
    assert!(
        size_of::<queues::Retry<'static>>()
            == size_of::<Generation>() + size_of::<clock::Deadline<'static>>()
    );
    assert!(
        size_of::<backoff::Backoff<'static, 'static, u32, u32, true>>()
            == size_of::<Unlinked<'static, 'static, u32, u32>>()
    );
    assert!(
        size_of::<failure::Failure<'static, 'static, u32, u32>>()
            == size_of::<Unlinked<'static, 'static, u32, u32>>()
    );

    let mut health = Health::new();
    assert!(health.try_ready(Ready::Probe).is_ok());
    assert!(health.generation.get() == 1);
    assert!(matches!(health.ready, Ready::Probe));
    assert!(matches!(health.idle, Idle::Exhausted));
    assert!(health.try_ready(Ready::Healthy).is_ok());
    assert!(health.generation.get() == 2);
    assert!(matches!(health.ready, Ready::Healthy));

    health.generation = Generation::LAST;
    health.ready = Ready::Probe;
    health.idle = Idle::Probing;
    assert!(health.try_ready(Ready::Healthy).is_err());
    assert!(health.generation.get() == u64::MAX);
    assert!(matches!(health.idle, Idle::Exhausted));
    assert!(matches!(health.ready, Ready::Probe));
    health.idle = Idle::Backoff;
    assert!(health.try_ready(Ready::Probe).is_err());
    assert!(health.generation.get() == u64::MAX);
    assert!(matches!(health.idle, Idle::Exhausted));
    assert!(matches!(health.ready, Ready::Probe));
    assert!(health.try_ready(Ready::Healthy).is_err());
    assert!(health.generation.get() == u64::MAX);
    assert!(matches!(health.idle, Idle::Exhausted));
};
