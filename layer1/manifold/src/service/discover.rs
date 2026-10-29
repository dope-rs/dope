use std::marker::PhantomData;

use dope_core::driver::schedule::clock;
use o3::base::cell::region;

use crate::service::{self, health};

/// Why a discovery refresh was requested by its owner.
pub enum Reason {
    Startup,
    Failure(health::Cause),
}

/// One bounded discovery state-machine transition.
pub enum Action<'d, I, A, M, E, const N: usize> {
    Pending {
        poll_at: Option<clock::Deadline<'d>>,
    },
    Published {
        snapshot: service::Snapshot<service::Endpoint<I, A>, N>,
        metadata: M,
        poll_at: Option<clock::Deadline<'d>>,
    },
    /// The current snapshot is no longer fresh but remains the desired set.
    Stale {
        revision: service::Revision,
        poll_at: Option<clock::Deadline<'d>>,
    },
    /// The current snapshot is no longer desired and must be withdrawn.
    Expired {
        revision: service::Revision,
        poll_at: Option<clock::Deadline<'d>>,
    },
    Failed {
        error: E,
        poll_at: Option<clock::Deadline<'d>>,
    },
}

/// Bounded endpoint discovery; `poll_at` schedules its next wake-up. Callbacks
/// share one admitted maintenance transition, so longer work must finish
/// externally before publishing a snapshot.
pub trait Output {
    type Metadata<'d>;
    type Error;
}

/// Current demand for an installed, immutable endpoint set.
pub trait Demand<'d> {
    type Endpoint;

    fn is_needed(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool;

    /// Admits a future dial without withdrawing an already submitted epoch.
    fn is_eligible(
        &self,
        endpoint: &Self::Endpoint,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> bool;
}

pub trait RefreshAt<'d> {
    fn refresh_at(&mut self, now: clock::Moment<'d>, reason: Reason);
}

pub trait PollAt<'d, I, A, const N: usize>: Output {
    /// Reports externally completed work before the scheduled deadline.
    /// Polling consumes or advances the ready work.
    fn is_ready(&self, _: &region::Token<region::Identity<'d, 1>>) -> bool {
        false
    }

    fn poll_at(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: clock::Moment<'d>,
    ) -> Action<'d, I, A, Self::Metadata<'d>, Self::Error, N>;
}

pub struct Silent;

pub struct Notify {
    pub(crate) cause: Option<health::Cause>,
}

/// Owns the demand used for both wake reconciliation and immediate dial admission.
pub struct Gate<D> {
    enabled: bool,
    source: D,
}

/// Selects demand-driven discovery; the controller owns the sole source in `Gate<D>`.
pub struct Watched<D>(PhantomData<fn() -> D>);

pub struct Refreshing<'d, D> {
    source: D,
    poll_at: Option<clock::Deadline<'d>>,
}

pub trait Driver<'d> {
    type Feedback: Access;
}

pub trait Access {
    fn is_enabled(&self) -> bool;
}

pub(crate) trait Eligibility<'d, I>: Access {
    const WATCHED: bool;

    fn is_eligible(&self, endpoint: &I, token: &region::Token<region::Identity<'d, 1>>) -> bool;
}

pub(crate) trait Feedback: Access {
    fn cause_mut(&mut self) -> Option<&mut Option<health::Cause>>;
}

impl Notify {
    pub(crate) const fn new() -> Self {
        Self { cause: None }
    }

    pub(crate) fn take(&mut self) -> Option<health::Cause> {
        self.cause.take()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.cause.is_some()
    }
}

impl<D> Gate<D> {
    pub(crate) const fn new(source: D) -> Self {
        Self {
            enabled: false,
            source,
        }
    }

    pub(crate) fn source(&self) -> &D {
        &self.source
    }

    pub(crate) fn set(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

impl<D> Watched<D> {
    pub(crate) const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<'d, D: RefreshAt<'d>> Refreshing<'d, D> {
    pub(crate) fn new(mut source: D, now: clock::Moment<'d>) -> Self {
        source.refresh_at(now, Reason::Startup);
        Self {
            source,
            poll_at: Some(now.into()),
        }
    }
}

impl<'d, D: Demand<'d>> Driver<'d> for Watched<D> {
    type Feedback = Gate<D>;
}

impl<'d, D: Output> Driver<'d> for Refreshing<'d, D> {
    type Feedback = Notify;
}

impl<'d, D> Refreshing<'d, D> {
    pub(crate) fn source(&self) -> &D {
        &self.source
    }

    pub(crate) fn source_mut(&mut self) -> &mut D {
        &mut self.source
    }

    pub(crate) fn poll_at(&self) -> Option<clock::Deadline<'d>> {
        self.poll_at
    }

    pub(crate) fn schedule(&mut self, next: Option<clock::Deadline<'d>>) {
        self.poll_at = next;
    }
}

impl Access for Silent {
    fn is_enabled(&self) -> bool {
        true
    }
}

impl Feedback for Silent {
    fn cause_mut(&mut self) -> Option<&mut Option<health::Cause>> {
        None
    }
}

impl Feedback for Notify {
    fn cause_mut(&mut self) -> Option<&mut Option<health::Cause>> {
        Some(&mut self.cause)
    }
}

impl Access for Notify {
    fn is_enabled(&self) -> bool {
        true
    }
}

impl<D> Access for Gate<D> {
    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl<D> Feedback for Gate<D> {
    fn cause_mut(&mut self) -> Option<&mut Option<health::Cause>> {
        None
    }
}

impl<'d, I> Eligibility<'d, I> for Silent {
    const WATCHED: bool = false;

    fn is_eligible(&self, _: &I, _: &region::Token<region::Identity<'d, 1>>) -> bool {
        true
    }
}

impl<'d, I> Eligibility<'d, I> for Notify {
    const WATCHED: bool = false;

    fn is_eligible(&self, _: &I, _: &region::Token<region::Identity<'d, 1>>) -> bool {
        true
    }
}

impl<'d, I, D: Demand<'d, Endpoint = I>> Eligibility<'d, I> for Gate<D> {
    const WATCHED: bool = true;

    fn is_eligible(&self, endpoint: &I, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        self.source.is_eligible(endpoint, token)
    }
}
