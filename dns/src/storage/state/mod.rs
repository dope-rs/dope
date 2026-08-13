use std::{io, time};

use crate::{
    discovery,
    storage::{failure, queries, timestamp},
    wire::{self, response},
};

pub(super) mod indices;
pub(super) mod streams;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Family {
    V4 = 0,
    V6 = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Transport {
    Datagram,
    Stream,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct RetryPolicy {
    attempts: u8,
    servers: u8,
}

impl Family {
    pub(super) const ALL: [Self; 2] = [Self::V4, Self::V6];
    const COUNT: usize = Self::ALL.len();

    const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn record_type(self) -> u16 {
        match self {
            Self::V4 => wire::A,
            Self::V6 => wire::AAAA,
        }
    }

    const fn from_bit(bit: u16) -> Self {
        if bit == 0 { Self::V4 } else { Self::V6 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Key {
    pub(super) lane: indices::LaneIndex,
    pub(super) generation: u64,
}

pub(crate) struct Answer<const N: usize> {
    pub(crate) addresses: crate::Addresses<N>,
    pub(crate) started_at: time::Instant,
    pub(crate) completed_at: time::Instant,
    pub(crate) times: failure::FailureTimes,
    pub(crate) valid_until: time::Instant,
}

pub(crate) enum Poll<const N: usize> {
    Pending(Option<time::Instant>),
    Complete(Result<Answer<N>, crate::Failure>),
}

pub(super) struct Request {
    pub(super) name: crate::Name,
    pub(super) alias_ttl: u32,
    pub(super) alias_hops: u8,
    pub(super) attempt: u8,
    server: indices::ServerIndex,
    mode: streams::Mode,
}

const _: () = assert!(std::mem::size_of::<streams::Mode>() == std::mem::size_of::<Transport>());
const _: () = assert!(std::mem::align_of::<streams::Mode>() == std::mem::align_of::<Transport>());
const _: () = assert!(std::mem::size_of::<RetryPolicy>() == 2);

impl RetryPolicy {
    pub(super) fn new(attempts: u8, servers: u8) -> Self {
        debug_assert!(attempts != 0);
        debug_assert!(servers != 0);
        Self { attempts, servers }
    }

    const fn can_retry(self, attempt: u8) -> bool {
        attempt + 1 < self.attempts
    }

    const fn servers(self) -> usize {
        self.servers as usize
    }
}

pub(super) enum FamilyState<const N: usize> {
    Pending(Request),
    Claimed,
    Waiting {
        request: Request,
        transaction: queries::TransactionLease,
        deadline: time::Instant,
    },
    Done {
        addresses: crate::Addresses<N>,
        ttl: u32,
    },
    Empty,
    Failed(discovery::ErrorKind),
}

pub(super) struct Query<const N: usize> {
    pub(super) generation: u64,
    pub(super) started_at: time::Instant,
    pub(super) started_unix_ns: timestamp::Timestamp,
    families: [FamilyState<N>; Family::COUNT],
}

pub(super) enum State<const N: usize> {
    Vacant {
        generation: u64,
    },
    Query(Query<N>),
    Complete {
        generation: u64,
        result: Result<Answer<N>, crate::Failure>,
    },
}

impl<const N: usize> State<N> {
    pub(super) const fn vacant() -> Self {
        Self::Vacant { generation: 0 }
    }

    pub(super) fn generation(&self) -> u64 {
        match self {
            Self::Vacant { generation } | Self::Complete { generation, .. } => *generation,
            Self::Query(query) => query.generation,
        }
    }

    pub(super) fn next_generation(&self) -> io::Result<u64> {
        self.generation()
            .checked_add(1)
            .ok_or_else(|| io::Error::other("DNS query generation exhausted"))
    }
}

impl Request {
    fn new(name: crate::Name) -> Self {
        Self {
            name,
            alias_ttl: u32::MAX,
            alias_hops: 0,
            attempt: 0,
            server: indices::ServerIndex::ZERO,
            mode: streams::Mode::Datagram,
        }
    }

    pub(super) const fn server(&self) -> indices::ServerIndex {
        self.server
    }

    pub(super) const fn transport(&self) -> Transport {
        match &self.mode {
            streams::Mode::Datagram => Transport::Datagram,
            streams::Mode::Stream(_) => Transport::Stream,
        }
    }

    pub(super) fn release(self, streams: &mut impl streams::Registry) {
        if let streams::Mode::Stream(lease) = self.mode {
            streams.release(lease, self.server);
        }
    }

    fn retry(mut self, servers: usize, streams: &mut impl streams::Registry) -> Self {
        let previous = self.server;
        self.attempt += 1;
        self.server = previous.next(servers);
        self.mode = match self.mode {
            streams::Mode::Datagram => streams::Mode::Datagram,
            streams::Mode::Stream(lease) => {
                streams::Mode::Stream(streams.transfer(lease, previous, self.server))
            }
        };
        self
    }
}

impl<const N: usize> FamilyState<N> {
    pub(super) fn new(name: crate::Name) -> Self {
        Self::Pending(Request::new(name))
    }

    pub(super) fn waiting(
        &self,
        expected_id: wire::TransactionId,
        expected_transport: Transport,
    ) -> Option<&Request> {
        let Self::Waiting {
            request,
            transaction,
            ..
        } = self
        else {
            return None;
        };
        if transaction.id() != expected_id || request.transport() != expected_transport {
            return None;
        }
        Some(request)
    }

    pub(super) fn timeout(
        request: Request,
        policy: RetryPolicy,
        streams: &mut impl streams::Registry,
    ) -> Self {
        Self::retry_or_fail(request, policy, streams, discovery::ErrorKind::Timeout)
    }

    pub(super) fn malformed(
        request: Request,
        policy: RetryPolicy,
        streams: &mut impl streams::Registry,
    ) -> Self {
        Self::retry_or_fail(request, policy, streams, discovery::ErrorKind::Malformed)
    }

    fn retry_or_fail(
        request: Request,
        policy: RetryPolicy,
        streams: &mut impl streams::Registry,
        exhausted: discovery::ErrorKind,
    ) -> Self {
        if policy.can_retry(request.attempt) {
            return Self::Pending(request.retry(policy.servers(), streams));
        }
        Self::finish(request, streams, Self::Failed(exhausted))
    }

    fn finish(request: Request, streams: &mut impl streams::Registry, next: Self) -> Self {
        request.release(streams);
        next
    }

    pub(super) fn reject(
        rejection: response::Rejection,
        request: Request,
        streams: &mut impl streams::Registry,
    ) -> Self {
        let kind = match rejection {
            response::Rejection::Capacity { actual } => discovery::ErrorKind::Capacity {
                limit: N,
                actual: usize::from(actual),
            },
            response::Rejection::CnameRecords
            | response::Rejection::CnameDepth
            | response::Rejection::InvalidAlias => discovery::ErrorKind::Malformed,
        };
        Self::finish(request, streams, Self::Failed(kind))
    }

    pub(super) fn apply(
        outcome: wire::Outcome<N>,
        request: Request,
        policy: RetryPolicy,
        streams: &mut impl streams::Registry,
    ) -> Self {
        match outcome {
            wire::Outcome::Addresses {
                values,
                ttl,
                alias_hops,
            } => {
                let next = match cumulative_alias_hops(request.alias_hops, alias_hops) {
                    Some(_) => Self::Done {
                        addresses: values,
                        ttl: ttl.min(request.alias_ttl),
                    },
                    None => Self::Failed(discovery::ErrorKind::Malformed),
                };
                Self::finish(request, streams, next)
            }
            wire::Outcome::Alias { name, ttl, hops } => {
                match cumulative_alias_hops(request.alias_hops, hops) {
                    Some(alias_hops) => Self::Pending(Request {
                        name,
                        alias_ttl: ttl.min(request.alias_ttl),
                        alias_hops,
                        attempt: 0,
                        server: request.server,
                        mode: request.mode,
                    }),
                    None => Self::finish(
                        request,
                        streams,
                        Self::Failed(discovery::ErrorKind::Malformed),
                    ),
                }
            }
            wire::Outcome::NoData => Self::finish(request, streams, Self::Empty),
            wire::Outcome::Truncated => Self::truncated(request, streams),
            wire::Outcome::Failed(wire::Rcode::Name) => {
                Self::finish(request, streams, Self::Failed(discovery::ErrorKind::Name))
            }
            wire::Outcome::Failed(wire::Rcode::Refused) => {
                Self::retry_or_fail(request, policy, streams, discovery::ErrorKind::Refused)
            }
            wire::Outcome::Failed(_) => {
                Self::retry_or_fail(request, policy, streams, discovery::ErrorKind::Server)
            }
        }
    }

    fn truncated(request: Request, streams: &mut impl streams::Registry) -> Self {
        let Request {
            name,
            alias_ttl,
            alias_hops,
            attempt,
            server,
            mode,
        } = request;
        match mode {
            streams::Mode::Datagram => Self::Pending(Request {
                name,
                alias_ttl,
                alias_hops,
                attempt,
                server,
                mode: streams::Mode::Stream(streams.acquire(server)),
            }),
            streams::Mode::Stream(lease) => {
                streams.release(lease, server);
                Self::Failed(discovery::ErrorKind::Truncated)
            }
        }
    }

    pub(super) fn terminal(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Empty | Self::Failed(_))
    }

    pub(super) fn deadline(&self, now: time::Instant) -> Option<time::Instant> {
        match self {
            Self::Waiting { deadline, .. } => Some(*deadline),
            Self::Pending(_) => Some(now),
            Self::Claimed | Self::Done { .. } | Self::Empty | Self::Failed(_) => None,
        }
    }
}

fn cumulative_alias_hops(current: u8, additional: u8) -> Option<u8> {
    current
        .checked_add(additional)
        .filter(|total| usize::from(*total) <= wire::MAX_CNAME_DEPTH)
}

impl<const N: usize> Query<N> {
    pub(super) fn new(
        generation: u64,
        started_at: time::Instant,
        started_unix_ns: timestamp::Timestamp,
        name: &crate::Name,
    ) -> Self {
        Self {
            generation,
            started_at,
            started_unix_ns,
            families: Family::ALL.map(|_| FamilyState::new(name.clone())),
        }
    }

    pub(super) const fn family(&self, family: Family) -> &FamilyState<N> {
        &self.families[family.index()]
    }

    pub(super) const fn family_mut(&mut self, family: Family) -> &mut FamilyState<N> {
        &mut self.families[family.index()]
    }

    pub(super) fn into_families(self) -> impl Iterator<Item = (Family, FamilyState<N>)> {
        Family::ALL.into_iter().zip(self.families)
    }

    pub(super) fn complete(&self, now: time::Instant) -> Result<Answer<N>, crate::Failure> {
        let mut addresses = crate::Addresses::new();
        let mut ttl = None;
        let mut failure = None;
        for state in &self.families {
            match state {
                FamilyState::Done {
                    addresses: values,
                    ttl: family_ttl,
                } => {
                    ttl = Some(match ttl {
                        Some(current) => u32::min(current, *family_ttl),
                        None => *family_ttl,
                    });
                    for &value in values.as_slice() {
                        match addresses.try_insert_unique(value) {
                            Ok(_) => {}
                            Err(_) => {
                                return Err(self.failure(discovery::ErrorKind::Capacity {
                                    limit: N,
                                    actual: addresses.len() + 1,
                                }));
                            }
                        }
                    }
                }
                FamilyState::Failed(kind) => {
                    if failure.is_none() {
                        failure = Some(*kind);
                    }
                }
                FamilyState::Empty => {}
                FamilyState::Pending(_) | FamilyState::Claimed | FamilyState::Waiting { .. } => {
                    return Err(self.failure(discovery::ErrorKind::Malformed));
                }
            }
        }
        let completed_unix_ns = timestamp::Timestamp::now();
        let times = failure::FailureTimes::new(self.started_unix_ns, completed_unix_ns);
        if let Some(kind) = failure {
            return Err(crate::Failure::new(kind, times));
        }
        if addresses.is_empty() {
            return Err(crate::Failure::new(discovery::ErrorKind::Empty, times));
        }
        let Some(ttl) = ttl else {
            return Err(self.failure(discovery::ErrorKind::Malformed));
        };
        let Some(valid_until) = now.checked_add(time::Duration::from_secs(u64::from(ttl))) else {
            return Err(self.failure(discovery::ErrorKind::Deadline));
        };
        Ok(Answer {
            addresses,
            started_at: self.started_at,
            completed_at: now,
            times,
            valid_until,
        })
    }

    pub(super) fn terminal(&self) -> bool {
        self.families.iter().all(FamilyState::terminal)
    }

    pub(super) fn deadline(&self, now: time::Instant) -> Option<time::Instant> {
        self.families
            .iter()
            .filter_map(|family| family.deadline(now))
            .min()
    }

    fn failure(&self, kind: discovery::ErrorKind) -> crate::Failure {
        crate::Failure::new(
            kind,
            failure::FailureTimes::new(self.started_unix_ns, timestamp::Timestamp::now()),
        )
    }
}
