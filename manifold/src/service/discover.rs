use std::time;

use crate::service::{self, health};

/// Why a discovery refresh was requested by its owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refresh {
    Startup,
    Failure(health::Cause),
}

/// One bounded discovery state-machine transition.
pub enum Action<I, A, M, E, const N: usize> {
    Pending {
        poll_at: Option<time::Instant>,
    },
    Published {
        snapshot: service::Snapshot<service::Endpoint<I, A>, N>,
        metadata: M,
        poll_at: Option<time::Instant>,
    },
    Expired {
        revision: service::Revision,
        poll_at: Option<time::Instant>,
    },
    Failed {
        error: E,
        poll_at: Option<time::Instant>,
    },
}

/// Bounded endpoint discovery; `poll_at` schedules its next wake-up. Callbacks
/// share one admitted maintenance transition, so longer work must finish
/// externally before publishing a snapshot.
pub trait Discover<I, A, const N: usize> {
    type Metadata;
    type Error;

    /// Whether [`Self::changed`] can request an immediate poll without a
    /// previously returned deadline. `false` removes the check at
    /// monomorphization.
    const REACTIVE: bool = false;

    /// Reports that the source's desired snapshot differs from the last
    /// published snapshot.
    fn changed(&self) -> bool {
        false
    }

    fn refresh(&mut self, now: time::Instant, reason: Refresh);

    fn poll(&mut self, now: time::Instant) -> Action<I, A, Self::Metadata, Self::Error, N>;
}
