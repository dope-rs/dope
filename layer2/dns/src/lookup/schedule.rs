use dope::core::driver::schedule::clock;

use crate::lookup;

enum State<'d> {
    Idle {
        next: Option<clock::Deadline<'d>>,
    },
    Fresh {
        next: clock::Deadline<'d>,
        until: clock::Deadline<'d>,
        stale_until: clock::Deadline<'d>,
    },
    Stale {
        next: clock::Deadline<'d>,
        until: clock::Deadline<'d>,
    },
}

pub(super) enum Age<'d> {
    Fresh,
    Stale { next: clock::Deadline<'d> },
    Expired,
}

#[derive(Clone, Copy)]
pub(super) struct Retry(u8);

pub(super) struct Schedule<'d> {
    pub(super) requested: Option<lookup::Reason>,
    pub(super) refresh_at: Option<clock::Deadline<'d>>,
    state: State<'d>,
    failures: Option<Retry>,
}

impl<'d> Schedule<'d> {
    pub(super) const fn new() -> Self {
        Self {
            requested: None,
            refresh_at: None,
            state: State::Idle { next: None },
            failures: Some(Retry::ZERO),
        }
    }

    pub(super) fn request(&mut self, reason: lookup::Reason) {
        let pending = matches!(
            self.requested,
            Some(
                lookup::Reason::ConnectFailure(_)
                    | lookup::Reason::ConnectTimeout
                    | lookup::Reason::TransportFailure
                    | lookup::Reason::IdleFailure
            )
        );
        if !self.is_active()
            || pending && matches!(reason, lookup::Reason::Startup | lookup::Reason::Scheduled)
        {
            return;
        }
        self.requested = Some(reason);
    }

    pub(super) const fn deadline(&self) -> Option<clock::Deadline<'d>> {
        match &self.state {
            State::Idle { .. } => None,
            State::Fresh { until, .. } | State::Stale { until, .. } => Some(*until),
        }
    }

    pub(super) const fn next(&self) -> Option<clock::Deadline<'d>> {
        if !self.is_active() {
            return None;
        }
        match &self.state {
            State::Idle { next } => *next,
            State::Fresh { next, .. } | State::Stale { next, .. } => Some(*next),
        }
    }

    pub(super) fn delay(&mut self, due: clock::Deadline<'d>) {
        match &mut self.state {
            State::Idle { next } => *next = Some(due),
            State::Fresh { next, .. } | State::Stale { next, .. } => *next = due,
        }
    }

    pub(super) fn allow(&mut self, due: clock::Deadline<'d>) {
        if !self.is_active() {
            return;
        }
        if let State::Idle { next } = &mut self.state {
            next.get_or_insert(due);
        }
    }

    pub(super) fn fail(&mut self) -> Option<Retry> {
        let retry = self.failures?;
        let mut next = retry;
        if next.0 < Retry::LIMIT {
            next.0 += 1;
        }
        self.failures = Some(next);
        Some(retry)
    }

    pub(super) const fn is_active(&self) -> bool {
        self.failures.is_some()
    }

    pub(super) fn stop(&mut self) {
        self.failures = None;
        self.requested = None;
        self.refresh_at = None;
    }

    pub(super) fn advance(&mut self, now: clock::Moment<'d>) -> Age<'d> {
        match &self.state {
            State::Idle { .. } => Age::Fresh,
            State::Fresh {
                next,
                until,
                stale_until: stale,
            } => {
                if stale.is_due(now) {
                    Age::Expired
                } else if until.is_due(now) {
                    let next = *next;
                    self.state = State::Stale {
                        next,
                        until: *stale,
                    };
                    Age::Stale { next }
                } else {
                    Age::Fresh
                }
            }
            State::Stale { until, .. } => {
                if until.is_due(now) {
                    Age::Expired
                } else {
                    Age::Fresh
                }
            }
        }
    }

    pub(super) fn install(
        &mut self,
        next: clock::Deadline<'d>,
        at: Option<clock::Deadline<'d>>,
        fresh: Option<(clock::Deadline<'d>, clock::Deadline<'d>)>,
    ) {
        self.failures = Some(Retry::ZERO);
        self.refresh_at = at;
        self.state = match fresh {
            Some((until, stale)) => State::Fresh {
                next,
                until,
                stale_until: stale,
            },
            None => State::Idle { next: Some(next) },
        };
    }

    pub(super) fn expire(&mut self, now: clock::Moment<'d>) {
        self.state = State::Idle { next: self.next() };
        self.refresh_at = self.is_active().then_some(now.into());
    }
}

impl Retry {
    const ZERO: Self = Self(0);
    const LIMIT: u8 = 16;

    pub(super) const fn factor(self) -> u32 {
        1_u32 << self.0
    }
}
