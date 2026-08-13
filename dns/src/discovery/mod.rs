use std::{error, fmt, net, time};

use dope::manifold::service::{self, discover, health};
use storage::{queries, state};

use crate::{
    config,
    storage::{self, failure, timestamp},
};

mod query;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Timeout,
    Name,
    Refused,
    Server,
    Truncated,
    Empty,
    Capacity { limit: usize, actual: usize },
    Deadline,
    Malformed,
}

/// Why DNS issued a lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    Startup,
    Expired,
    ConnectFailure,
    TransportFailure,
}

impl Reason {
    const fn is_failure(self) -> bool {
        matches!(self, Self::ConnectFailure | Self::TransportFailure)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    pub reason: Reason,
    pub started_at: time::Instant,
    pub completed_at: time::Instant,
    pub started_unix_ns: timestamp::Timestamp,
    pub completed_unix_ns: timestamp::Timestamp,
    pub valid_until: time::Instant,
    pub refresh_at: Option<time::Instant>,
}

impl Metadata {
    pub fn query_duration(self) -> Result<time::Duration, TimeOrder> {
        self.completed_at
            .checked_duration_since(self.started_at)
            .ok_or(TimeOrder)
    }

    pub fn ttl_remaining(self) -> Result<time::Duration, TimeOrder> {
        self.valid_until
            .checked_duration_since(self.completed_at)
            .ok_or(TimeOrder)
    }

    pub fn refresh_in(self) -> Result<Option<time::Duration>, TimeOrder> {
        match self.refresh_at {
            Some(at) => at
                .checked_duration_since(self.completed_at)
                .map(Some)
                .ok_or(TimeOrder),
            None => Ok(None),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeOrder;

impl fmt::Display for TimeOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DNS timestamps are not monotonically ordered")
    }
}

impl error::Error for TimeOrder {}

#[derive(Debug)]
pub struct Error {
    target: crate::Target,
    kind: ErrorKind,
    pub started_unix_ns: timestamp::Timestamp,
    pub completed_unix_ns: timestamp::Timestamp,
}

impl Error {
    pub fn target(&self) -> &crate::Target {
        &self.target
    }

    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DNS lookup for {} failed: ", self.target)?;
        match self.kind {
            ErrorKind::Timeout => formatter.write_str("query timed out"),
            ErrorKind::Name => formatter.write_str("name does not exist"),
            ErrorKind::Refused => formatter.write_str("server refused the query"),
            ErrorKind::Server => formatter.write_str("server failed the query"),
            ErrorKind::Truncated => formatter.write_str("DNS-over-TCP response was truncated"),
            ErrorKind::Empty => formatter.write_str("answer contained no addresses"),
            ErrorKind::Capacity { limit, actual } => {
                write!(
                    formatter,
                    "answer contained {actual} addresses; limit is {limit}"
                )
            }
            ErrorKind::Deadline => {
                formatter.write_str("deadline exceeds the monotonic clock range")
            }
            ErrorKind::Malformed => formatter.write_str("response or resolver state is malformed"),
        }
    }
}

impl error::Error for Error {}

#[expect(
    clippy::large_enum_variant,
    reason = "a thread-local discovery owns its bounded query without allocation or indirection"
)]
enum Source<'d, const LANES: usize, const ADDRESSES: usize> {
    Literal(net::IpAddr),
    Query(query::Query<'d, LANES, ADDRESSES>),
}

struct Schedule {
    requested: Option<Reason>,
    next_allowed: time::Instant,
    refresh_at: Option<time::Instant>,
    valid_until: Option<time::Instant>,
    failures: u32,
}

pub struct Discovery<'d, const LANES: usize, const ADDRESSES: usize> {
    source: Source<'d, LANES, ADDRESSES>,
    port: u16,
    refresh: &'d config::Refresh,
    revision: service::Revision,
    schedule: Schedule,
}

const _: () = assert!(std::mem::size_of::<&config::Refresh>() == std::mem::size_of::<usize>());

impl<'d, const LANES: usize, const ADDRESSES: usize> Discovery<'d, LANES, ADDRESSES> {
    pub(crate) fn literal(ip: net::IpAddr, port: u16, refresh: &'d config::Refresh) -> Self {
        Self::new(Source::Literal(ip), port, refresh)
    }

    pub(crate) fn query(
        name: crate::Name,
        port: u16,
        lane: queries::Lane<'d, LANES, ADDRESSES>,
        refresh: &'d config::Refresh,
    ) -> Self {
        Self::new(Source::Query(query::Query::new(name, lane)), port, refresh)
    }

    fn new(source: Source<'d, LANES, ADDRESSES>, port: u16, refresh: &'d config::Refresh) -> Self {
        let now = time::Instant::now();
        Self {
            source,
            port,
            refresh,
            revision: service::Revision::INITIAL,
            schedule: Schedule {
                requested: None,
                next_allowed: now,
                refresh_at: None,
                valid_until: None,
                failures: 0,
            },
        }
    }

    pub fn target(&self) -> crate::TargetRef<'_> {
        match &self.source {
            Source::Literal(ip) => crate::TargetRef::literal(*ip, self.port),
            Source::Query(query) => crate::TargetRef::named(query.name(), self.port),
        }
    }

    fn request(&mut self, reason: Reason) {
        if self.schedule.requested.is_some_and(Reason::is_failure) && !reason.is_failure() {
            return;
        }
        self.schedule.requested = Some(reason);
    }

    fn retry_delay(&self) -> time::Duration {
        let shift = match self.schedule.failures.checked_sub(1) {
            Some(value) => value.min(16),
            None => 0,
        };
        let multiplier = 1u32 << shift;
        match self.refresh.backoff.base.checked_mul(multiplier) {
            Some(delay) => delay.min(self.refresh.backoff.max),
            None => self.refresh.backoff.max,
        }
    }

    fn next_poll(&self, proposed: Option<time::Instant>) -> Option<time::Instant> {
        match (proposed, self.schedule.valid_until) {
            (Some(proposed), Some(valid_until)) => Some(proposed.min(valid_until)),
            (Some(proposed), None) => Some(proposed),
            (None, Some(valid_until)) => Some(valid_until),
            (None, None) => None,
        }
    }

    fn failed(
        &mut self,
        now: time::Instant,
        reason: Reason,
        failure: crate::Failure,
    ) -> discover::Action<net::SocketAddr, net::SocketAddr, Metadata, Error, ADDRESSES> {
        if self.schedule.failures < 17 {
            self.schedule.failures += 1;
        }
        let Some(poll_at) = now.checked_add(self.retry_delay()) else {
            self.schedule.requested = None;
            self.schedule.refresh_at = None;
            return discover::Action::Failed {
                error: Error {
                    target: self.target().into(),
                    kind: ErrorKind::Deadline,
                    started_unix_ns: failure.times.started_unix_ns,
                    completed_unix_ns: failure.times.completed_unix_ns,
                },
                poll_at: self.next_poll(None),
            };
        };
        self.request(reason);
        self.schedule.next_allowed = poll_at;
        discover::Action::Failed {
            error: Error {
                target: self.target().into(),
                kind: failure.kind,
                started_unix_ns: failure.times.started_unix_ns,
                completed_unix_ns: failure.times.completed_unix_ns,
            },
            poll_at: self.next_poll(Some(poll_at)),
        }
    }

    fn publish(
        &mut self,
        now: time::Instant,
        reason: Reason,
        answer: state::Answer<ADDRESSES>,
        periodic: bool,
    ) -> discover::Action<net::SocketAddr, net::SocketAddr, Metadata, Error, ADDRESSES> {
        let state::Answer {
            addresses,
            started_at,
            completed_at,
            times,
            valid_until,
        } = answer;
        let Some(revision) = self.revision.next() else {
            return self.failed(
                now,
                reason,
                crate::Failure::new(ErrorKind::Malformed, times),
            );
        };
        let address_count = addresses.len();
        let endpoints = addresses.into_iter().map(|ip| {
            let address = net::SocketAddr::new(ip, self.port);
            service::Endpoint::new(address, address)
        });
        let snapshot = match service::Snapshot::try_new(revision, endpoints) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                return self.failed(
                    now,
                    reason,
                    crate::Failure::new(
                        ErrorKind::Capacity {
                            limit: ADDRESSES,
                            actual: address_count,
                        },
                        times,
                    ),
                );
            }
        };
        let Some(next_allowed) = now.checked_add(self.refresh.minimum) else {
            return self.failed(now, reason, crate::Failure::new(ErrorKind::Deadline, times));
        };
        let Some(policy_until) = now.checked_add(self.refresh.maximum_ttl) else {
            return self.failed(now, reason, crate::Failure::new(ErrorKind::Deadline, times));
        };
        let effective_until = if periodic {
            valid_until.min(policy_until)
        } else {
            valid_until
        };
        let Some(_) = effective_until.checked_duration_since(now) else {
            return self.failed(now, reason, crate::Failure::new(ErrorKind::Deadline, times));
        };
        let refresh_at = if periodic {
            let preferred = effective_until
                .checked_sub(self.refresh.before)
                .unwrap_or(now);
            Some(preferred.max(next_allowed).min(effective_until))
        } else {
            None
        };
        self.revision = revision;
        self.schedule.failures = 0;
        self.schedule.next_allowed = next_allowed;
        self.schedule.refresh_at = refresh_at;
        self.schedule.valid_until = periodic.then_some(effective_until);
        discover::Action::Published {
            snapshot,
            metadata: Metadata {
                reason,
                started_at,
                completed_at,
                started_unix_ns: times.started_unix_ns,
                completed_unix_ns: times.completed_unix_ns,
                valid_until: effective_until,
                refresh_at,
            },
            poll_at: match self.schedule.requested {
                Some(_) => Some(self.schedule.next_allowed),
                None => refresh_at,
            },
        }
    }

    fn expire(
        &mut self,
        now: time::Instant,
    ) -> discover::Action<net::SocketAddr, net::SocketAddr, Metadata, Error, ADDRESSES> {
        self.schedule.valid_until = None;
        let Some(revision) = self.revision.next() else {
            let timestamp = timestamp::Timestamp::now();
            return discover::Action::Failed {
                error: Error {
                    target: self.target().into(),
                    kind: ErrorKind::Malformed,
                    started_unix_ns: timestamp,
                    completed_unix_ns: timestamp,
                },
                poll_at: None,
            };
        };
        self.revision = revision;
        discover::Action::Expired {
            revision,
            poll_at: Some(now),
        }
    }

    fn publish_literal(
        &mut self,
        now: time::Instant,
        reason: Reason,
        ip: net::IpAddr,
    ) -> discover::Action<net::SocketAddr, net::SocketAddr, Metadata, Error, ADDRESSES> {
        let unix = timestamp::Timestamp::now();
        let times = failure::FailureTimes::at(unix);
        let Some(valid_until) = now.checked_add(time::Duration::from_secs(86_400)) else {
            return self.failed(now, reason, crate::Failure::new(ErrorKind::Deadline, times));
        };
        self.publish(
            now,
            reason,
            state::Answer {
                addresses: crate::Addresses::singleton(ip),
                started_at: now,
                completed_at: now,
                times,
                valid_until,
            },
            false,
        )
    }
}

impl<'d, const LANES: usize, const ADDRESSES: usize>
    discover::Discover<net::SocketAddr, net::SocketAddr, ADDRESSES>
    for Discovery<'d, LANES, ADDRESSES>
{
    type Metadata = Metadata;
    type Error = Error;

    fn refresh(&mut self, _now: time::Instant, reason: discover::Refresh) {
        let reason = match reason {
            discover::Refresh::Startup => Reason::Startup,
            discover::Refresh::Failure(health::Cause::Connect) => Reason::ConnectFailure,
            discover::Refresh::Failure(health::Cause::Transport) => Reason::TransportFailure,
            discover::Refresh::Failure(
                health::Cause::Idle | health::Cause::Protocol | health::Cause::Remote,
            ) => return,
        };
        self.request(reason);
    }

    fn poll(
        &mut self,
        now: time::Instant,
    ) -> discover::Action<net::SocketAddr, net::SocketAddr, Self::Metadata, Self::Error, ADDRESSES>
    {
        if self
            .schedule
            .valid_until
            .is_some_and(|valid_until| valid_until <= now)
        {
            return self.expire(now);
        }
        let query_poll = match &mut self.source {
            Source::Literal(_) => query::QueryPoll::Idle,
            Source::Query(query) => query.poll(now),
        };
        match query_poll {
            query::QueryPoll::Idle => {}
            query::QueryPoll::Pending(poll_at) => {
                return discover::Action::Pending {
                    poll_at: self.next_poll(poll_at),
                };
            }
            query::QueryPoll::Complete {
                reason,
                result: Ok(answer),
            } => {
                return self.publish(now, reason, answer, true);
            }
            query::QueryPoll::Complete {
                reason,
                result: Err(error),
            } => {
                return self.failed(now, reason, error);
            }
        }
        if self.schedule.requested.is_none() && self.schedule.refresh_at.is_some_and(|at| at <= now)
        {
            self.request(Reason::Expired);
        }
        let Some(reason) = self.schedule.requested else {
            return discover::Action::Pending {
                poll_at: self.next_poll(self.schedule.refresh_at),
            };
        };
        if now < self.schedule.next_allowed {
            return discover::Action::Pending {
                poll_at: self.next_poll(Some(self.schedule.next_allowed)),
            };
        }
        let started_unix_ns = timestamp::Timestamp::now();
        let started = match &mut self.source {
            Source::Literal(ip) => {
                let ip = *ip;
                self.schedule.requested = None;
                return self.publish_literal(now, reason, ip);
            }
            Source::Query(query) => query.start(reason, now, started_unix_ns),
        };
        if let Err(error) = started {
            return self.failed(now, reason, error);
        }
        self.schedule.requested = None;
        discover::Action::Pending {
            poll_at: self.next_poll(Some(now)),
        }
    }
}
