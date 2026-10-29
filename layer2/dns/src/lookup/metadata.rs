use std::time::Duration;

use dope::core::driver::schedule::clock;
use o3::base::time::{Nanos, Unix};

use crate::{
    lookup::{self, error},
    storage::range,
};

/// What one completed lookup published: why, when, and for how long.
#[derive(Clone, Copy)]
pub struct Metadata {
    pub reason: lookup::Reason,
    pub started_unix_ns: Unix<Nanos>,
    pub completed_unix_ns: Unix<Nanos>,
    duration: Duration,
    ttl: Duration,
    refresh: Option<Duration>,
}

impl Metadata {
    /// Earlier replies may expire while another family or alias remains pending.
    pub(super) fn try_new<'d>(
        reason: lookup::Reason,
        times: range::Range,
        start: clock::Moment<'d>,
        done: clock::Moment<'d>,
        valid: clock::Deadline<'d>,
        at: Option<clock::Deadline<'d>>,
    ) -> Result<Self, error::Timing> {
        let duration = done.try_since(start).ok_or(error::Timing::Duration {
            started: start.into_raw(),
            completed: done.into_raw(),
        })?;
        let ttl = valid.into_raw().saturating_duration_since(done.into_raw());
        let refresh = at.map(|at| at.into_raw().saturating_duration_since(done.into_raw()));
        Ok(Self {
            reason,
            started_unix_ns: times.start,
            completed_unix_ns: times.done,
            duration,
            ttl,
            refresh,
        })
    }

    pub const fn duration(&self) -> Duration {
        self.duration
    }

    pub const fn ttl(&self) -> Duration {
        self.ttl
    }

    pub const fn refresh_in(&self) -> Option<Duration> {
        self.refresh
    }
}
