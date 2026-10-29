use std::{marker, net, num, time::Duration};

use dope::{
    core::driver::schedule::clock,
    manifold::service::{self, discover},
    runtime::civil,
};
use o3::{base::cell::region, depot::array};
use storage::queries::{self, state};

use crate::{
    config,
    query::answer::addr,
    storage::{self, range},
};

pub mod create;
pub mod error;
mod metadata;
mod query;
mod reason;
pub mod refresh;
mod schedule;
mod source;

pub use metadata::Metadata;
pub use reason::Reason;

pub struct Discovery<'storage, 's, 'd, P, const LANES: usize, const ADDRESSES: usize, B = ()> {
    source: source::Source<'storage, 's, 'd, LANES, ADDRESSES>,
    port: num::NonZero<u16>,
    refresh: &'storage config::Refresh,
    revision: service::Revision,
    schedule: schedule::Schedule<'d>,
    clock: civil::Anchor<'d>,
    policy: marker::PhantomData<fn() -> P>,
    bound: B,
}

impl<'storage, 's, 'd, P, const LANES: usize, const ADDRESSES: usize>
    Discovery<'storage, 's, 'd, P, LANES, ADDRESSES>
where
    P: refresh::Policy,
{
    pub(crate) fn bind<B>(self, bound: B) -> Discovery<'storage, 's, 'd, P, LANES, ADDRESSES, B> {
        let Self {
            source,
            port,
            refresh,
            revision,
            schedule,
            clock,
            policy,
            bound: (),
        } = self;
        Discovery {
            source,
            port,
            refresh,
            revision,
            schedule,
            clock,
            policy,
            bound,
        }
    }

    pub(crate) fn literal(
        address: net::SocketAddr,
        port: num::NonZero<u16>,
        cfg: &'storage config::Refresh,
        clock: civil::Anchor<'d>,
    ) -> Self {
        Self::new(source::Source::Literal(address), port, cfg, clock)
    }

    pub(crate) fn query(
        name: Box<crate::host::Name>,
        port: num::NonZero<u16>,
        lane: queries::Lane<'storage, 's, 'd, LANES, ADDRESSES>,
        cfg: &'storage config::Refresh,
        clock: civil::Anchor<'d>,
    ) -> Self {
        Self::new(
            source::Source::Query(query::Query::new(name, lane)),
            port,
            cfg,
            clock,
        )
    }

    fn new(
        src: source::Source<'storage, 's, 'd, LANES, ADDRESSES>,
        port: num::NonZero<u16>,
        cfg: &'storage config::Refresh,
        clock: civil::Anchor<'d>,
    ) -> Self {
        Self {
            source: src,
            port,
            refresh: cfg,
            revision: service::Revision::INITIAL,
            schedule: schedule::Schedule::new(),
            clock,
            policy: marker::PhantomData,
            bound: (),
        }
    }
}

impl<'storage, 's, 'd, P, const LANES: usize, const ADDRESSES: usize, B>
    Discovery<'storage, 's, 'd, P, LANES, ADDRESSES, B>
where
    P: refresh::Policy,
{
    pub fn target(&self) -> crate::TargetRef<'_> {
        match &self.source {
            source::Source::Literal(address) => crate::TargetRef::literal(*address, self.port),
            source::Source::Query(query) => crate::TargetRef::named(query.name(), self.port),
        }
    }

    fn retry_delay(&self, retry: schedule::Retry) -> Duration {
        let backoff = &self.refresh.backoff;
        backoff.base.saturating_mul(retry.factor()).min(backoff.max)
    }

    /// Reserves a withdrawal revision; expiry may advance it during a lookup.
    fn publication(&self) -> Result<service::Revision, service::Exhausted> {
        let next = self.revision.try_next()?;
        if matches!(self.source, source::Source::Query(_)) {
            next.try_next()?;
        }
        Ok(next)
    }

    fn next_poll(&self, next: Option<clock::Deadline<'d>>) -> Option<clock::Deadline<'d>> {
        match (next, self.schedule.deadline()) {
            (Some(next), Some(due)) => Some(next.min(due)),
            (next, None) | (None, next) => next,
        }
    }

    fn failed(
        &mut self,
        now: clock::Moment<'d>,
        reason: Reason,
        error: error::Error<ADDRESSES>,
    ) -> discover::Action<
        'd,
        net::SocketAddr,
        net::SocketAddr,
        Metadata,
        error::Error<ADDRESSES>,
        ADDRESSES,
    > {
        let due = if error.is_terminal() {
            self.schedule.stop();
            None
        } else {
            self.schedule.fail().map(|retry| {
                let due = now.clamp_add(self.retry_delay(retry));
                self.schedule.request(reason);
                self.schedule.delay(due);
                due
            })
        };
        discover::Action::Failed {
            error,
            poll_at: self.next_poll(due),
        }
    }

    fn publish(
        &mut self,
        now: clock::Moment<'d>,
        reason: Reason,
        answer: state::Answer<ADDRESSES>,
        times: range::Range,
    ) -> discover::Action<
        'd,
        net::SocketAddr,
        net::SocketAddr,
        Metadata,
        error::Error<ADDRESSES>,
        ADDRESSES,
    > {
        let state::Answer {
            addresses: addrs,
            started_at: start,
            completed_at: done,
            valid_until: until,
        } = answer;
        let query = matches!(self.source, source::Source::Query(_));
        let start = now.moment_at(start);
        let done = now.moment_at(done);
        let until = now.deadline_at(until);
        let rev = match self.publication() {
            Ok(rev) => rev,
            Err(source) => {
                return self.failed(
                    now,
                    reason,
                    error::Error::QueryRevision {
                        source,
                        started_unix_ns: times.start,
                        completed_unix_ns: times.done,
                    },
                );
            }
        };
        let addrs = match &self.source {
            source::Source::Literal(address) => {
                array::Vec::from_one(service::Endpoint::new(*address, *address))
            }
            source::Source::Query(_) => addrs.bind(self.port.get()),
        };
        let view = service::Snapshot::from_vec(rev, addrs);
        let timing = (|| -> Result<_, clock::Range> {
            let next = now.try_add(self.refresh.minimum)?;
            if !query {
                return Ok((next, until, None, None));
            }
            let cap = done.try_add(self.refresh.maximum_ttl)?;
            let valid = until.min(cap);
            let pref = match valid.checked_sub(self.refresh.before) {
                Some(pref) => pref,
                None => now.into(),
            };
            let at = Some(pref.max(next).min(valid));
            let fresh = Some((valid, valid.try_add(self.refresh.stale)?));
            Ok((next, valid, at, fresh))
        })();
        let (next, valid, at, fresh) = match timing {
            Ok(timing) => timing,
            Err(source) => {
                return self.failed(
                    now,
                    reason,
                    error::Error::query(error::Kind::Clock(source), times),
                );
            }
        };
        let metadata = match Metadata::try_new(reason, times, start, done, valid, at) {
            Ok(metadata) => metadata,
            Err(source) => {
                return self.failed(
                    now,
                    reason,
                    error::Error::query(error::Kind::Timing(source), times),
                );
            }
        };
        self.revision = rev;
        self.schedule.install(next, at, fresh);
        discover::Action::Published {
            snapshot: view,
            metadata,
            poll_at: match self.schedule.requested {
                Some(_) => self.next_poll(self.schedule.next()),
                None => self.next_poll(at),
            },
        }
    }

    fn expire(
        &mut self,
        now: clock::Moment<'d>,
    ) -> discover::Action<
        'd,
        net::SocketAddr,
        net::SocketAddr,
        Metadata,
        error::Error<ADDRESSES>,
        ADDRESSES,
    > {
        self.schedule.expire(now);
        let rev = match self.revision.try_next() {
            Ok(rev) => rev,
            Err(source) => {
                self.schedule.stop();
                return discover::Action::Failed {
                    error: error::Error::Revision { source },
                    poll_at: None,
                };
            }
        };
        self.revision = rev;
        discover::Action::Expired {
            revision: rev,
            poll_at: self.schedule.refresh_at,
        }
    }

    fn resolve(
        &mut self,
        now: clock::Moment<'d>,
        reason: Reason,
        address: net::SocketAddr,
    ) -> discover::Action<
        'd,
        net::SocketAddr,
        net::SocketAddr,
        Metadata,
        error::Error<ADDRESSES>,
        ADDRESSES,
    > {
        let unix = match self.clock.observe(&now) {
            Ok(stamp) => stamp,
            Err(source) => return self.failed(now, reason, error::Error::Clock { source }),
        };
        let times = range::Range::at(unix);
        let until = match now.try_add(Duration::from_secs(86_400)) {
            Ok(until) => until.into_raw(),
            Err(source) => {
                return self.failed(
                    now,
                    reason,
                    error::Error::query(error::Kind::Clock(source), times),
                );
            }
        };
        let raw = now.into_raw();
        let answer = state::Answer {
            addresses: addr::Addresses::singleton(address.ip()),
            started_at: raw,
            completed_at: raw,
            valid_until: until,
        };
        self.publish(now, reason, answer, times)
    }
}

impl<'storage, 's, 'd, P, const LANES: usize, const ADDRESSES: usize, B> discover::Output
    for Discovery<'storage, 's, 'd, P, LANES, ADDRESSES, B>
where
    P: refresh::Policy,
{
    type Metadata<'driver> = Metadata;
    type Error = error::Error<ADDRESSES>;
}

impl<'storage, 's, 'd, P, const LANES: usize, const ADDRESSES: usize, B> discover::RefreshAt<'d>
    for Discovery<'storage, 's, 'd, P, LANES, ADDRESSES, B>
where
    P: refresh::Policy,
{
    fn refresh_at(&mut self, now: clock::Moment<'d>, reason: discover::Reason) {
        let reason = match reason {
            discover::Reason::Startup => Reason::Startup,
            discover::Reason::Failure(cause) => {
                let Some(reason) = P::reason(cause) else {
                    return;
                };
                reason
            }
        };
        self.schedule.allow(now.into());
        self.schedule.request(reason);
    }
}

impl<'storage, 's, 'd, P, const LANES: usize, const ADDRESSES: usize, B>
    discover::PollAt<'d, net::SocketAddr, net::SocketAddr, ADDRESSES>
    for Discovery<'storage, 's, 'd, P, LANES, ADDRESSES, B>
where
    P: refresh::Policy,
{
    fn is_ready(&self, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        match &self.source {
            source::Source::Literal(_) => false,
            source::Source::Query(query) => query.is_ready(token),
        }
    }

    fn poll_at(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        now: clock::Moment<'d>,
    ) -> discover::Action<
        'd,
        net::SocketAddr,
        net::SocketAddr,
        Self::Metadata<'d>,
        Self::Error,
        ADDRESSES,
    > {
        let polled = match &mut self.source {
            source::Source::Literal(_) => query::QueryPoll::Idle,
            source::Source::Query(query) => query.poll(token, now),
        };
        let (busy, due) = match polled {
            query::QueryPoll::Idle => (false, None),
            query::QueryPoll::Pending(at) => (true, at),
            query::QueryPoll::Complete {
                reason,
                completed_at: done,
                started_unix_ns: start,
                result,
            } => {
                let done = match self.clock.observe(&done) {
                    Ok(stamp) => stamp,
                    Err(source) => {
                        let error = match result {
                            Ok(answer) => error::Error::AnswerClock {
                                completion: error::Completion::new(answer),
                                source,
                            },
                            Err(kind) => error::Error::QueryClock {
                                kind,
                                started_unix_ns: start,
                                source,
                            },
                        };
                        return self.failed(now, reason, error);
                    }
                };
                let times = range::Range::new(start, done);
                return match result {
                    Ok(answer) => self.publish(now, reason, answer, times),
                    Err(kind) => self.failed(now, reason, error::Error::query(kind, times)),
                };
            }
        };
        match self.schedule.advance(now) {
            schedule::Age::Fresh => {}
            schedule::Age::Stale { next } => {
                self.schedule.refresh_at = None;
                if !busy {
                    self.schedule.request(Reason::Scheduled);
                }
                let due = if busy {
                    self.next_poll(due)
                } else {
                    self.next_poll(self.schedule.is_active().then_some(next.max(now.into())))
                };
                return discover::Action::Stale {
                    revision: self.revision,
                    poll_at: due,
                };
            }
            schedule::Age::Expired => return self.expire(now),
        }
        if busy {
            return discover::Action::Pending {
                poll_at: self.next_poll(due),
            };
        }
        if self.schedule.requested.is_none()
            && self.schedule.refresh_at.is_some_and(|at| at.is_due(now))
        {
            self.schedule.request(Reason::Scheduled);
        }
        if let Some(next) = self.schedule.next().filter(|at| !at.is_due(now)) {
            return discover::Action::Pending {
                poll_at: self.next_poll(Some(next)),
            };
        }
        let Some(reason) = self.schedule.requested.take() else {
            return discover::Action::Pending {
                poll_at: self.next_poll(self.schedule.refresh_at),
            };
        };
        if let Err(source) = self.publication() {
            return self.failed(now, reason, error::Error::Revision { source });
        }
        let (start, stamp) = match &mut self.source {
            source::Source::Literal(address) => {
                let address = *address;
                return self.resolve(now, reason, address);
            }
            source::Source::Query(query) => {
                let stamp = match self.clock.observe(&now) {
                    Ok(stamp) => stamp,
                    Err(source) => return self.failed(now, reason, error::Error::Clock { source }),
                };
                (query.start(token, reason, now, stamp), stamp)
            }
        };
        if let Err(kind) = start {
            return self.failed(
                now,
                reason,
                error::Error::query(kind, range::Range::at(stamp)),
            );
        }
        discover::Action::Pending {
            poll_at: self.next_poll(Some(now.into())),
        }
    }
}
