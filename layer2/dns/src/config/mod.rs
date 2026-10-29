mod servers;

use std::{mem, net, num, time};

use o3::{base::time::Positive, depot::array::copy};

pub(crate) const MAX_SERVERS: usize = 4;
pub(crate) use servers::{Configured, Selected};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Error {
    OutboundBytes {
        outbound: usize,
        per_query: usize,
    },
    NoServers,
    ServerPort {
        server: net::SocketAddr,
    },
    ServerCount {
        limit: usize,
        server: net::SocketAddr,
    },
    ServerDuplicate {
        first: usize,
        duplicate: usize,
        server: net::SocketAddr,
    },
    QueryAttemptTimeoutZero,
    QueryTotalTimeoutZero,
    QueryTotalTimeoutShort {
        attempt: Positive,
        total: Positive,
    },
    QueryAttemptsZero,
    RefreshMinimumZero,
    RefreshBackoffBaseZero,
    RefreshBackoffOrder {
        base: time::Duration,
        maximum: time::Duration,
    },
    RefreshMaximumTtlZero,
    RefreshMinimumExceedsTtl {
        minimum: time::Duration,
        maximum: time::Duration,
    },
    RefreshLeadExceedsTtl {
        before: time::Duration,
        maximum: time::Duration,
    },
    AddressFamily {
        bind: net::SocketAddr,
        server: net::SocketAddr,
    },
    LaneBound {
        requested: usize,
        minimum: usize,
    },
    LaneAddressSpace {
        requested: usize,
        limit: usize,
    },
}

/// A bounded resolver list whose numeric lookups return exact failures.
#[derive(Clone, Copy)]
pub struct Servers {
    values: copy::Vec<net::SocketAddr, MAX_SERVERS>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexError {
    pub index: usize,
    pub count: usize,
}

impl Servers {
    pub const fn empty() -> Self {
        Self {
            values: copy::Vec::new(),
        }
    }

    pub fn try_new(addrs: impl IntoIterator<Item = net::SocketAddr>) -> Result<Self, Error> {
        let mut list = Self::empty();
        for server in addrs {
            list.try_push(server)?;
        }
        Ok(list)
    }

    /// Appends a server after checking its port, uniqueness, and bounded capacity.
    pub fn try_push(&mut self, server: net::SocketAddr) -> Result<(), Error> {
        if server.port() == 0 {
            return Err(Error::ServerPort { server });
        }
        let index = self.values.len();
        if let Some(first) = self.as_slice().iter().position(|seen| *seen == server) {
            return Err(Error::ServerDuplicate {
                first,
                duplicate: index,
                server,
            });
        }
        let Some(vacant) = self.values.vacancy() else {
            return Err(Error::ServerCount {
                limit: MAX_SERVERS,
                server,
            });
        };
        vacant.insert(server);
        Ok(())
    }
}

impl Servers {
    pub fn as_slice(&self) -> &[net::SocketAddr] {
        self.values.as_slice()
    }

    pub const fn len(&self) -> usize {
        self.values.extent().get()
    }

    pub const fn is_empty(&self) -> bool {
        self.values.extent().is_empty()
    }

    /// Borrows one configured address, retaining the successful selection.
    pub fn try_get(&self, index: usize) -> Result<&net::SocketAddr, IndexError> {
        self.as_slice().get(index).ok_or(IndexError {
            index,
            count: self.len(),
        })
    }
}

#[derive(Clone, Copy)]
pub struct Query<A = u8, D = time::Duration> {
    /// Maximum lifetime of one logical DNS attempt, including admission,
    /// transport fallback, connection establishment, and socket rotation.
    pub attempt_timeout: D,
    /// Hard end-to-end lifetime of one lookup across retries and aliases.
    pub total_timeout: D,
    pub attempts: A,
}

#[derive(Clone, Copy)]
pub struct Backoff {
    pub base: time::Duration,
    pub max: time::Duration,
}

#[derive(Clone, Copy)]
pub struct Refresh {
    pub minimum: time::Duration,
    pub backoff: Backoff,
    pub maximum_ttl: time::Duration,
    pub before: time::Duration,
    /// How long the installed snapshot remains desired after its DNS TTL.
    /// Zero requests withdrawal at the freshness deadline.
    pub stale: time::Duration,
}

#[derive(Clone, Copy)]
pub struct Config {
    pub bind: net::SocketAddr,
    pub servers: Servers,
    pub query: Query,
    pub refresh: Refresh,
}

#[derive(Clone, Copy)]
pub(crate) struct Validated {
    pub(crate) bind: net::SocketAddr,
    pub(crate) servers: Configured,
    pub(crate) query: Query<num::NonZero<u8>, Positive>,
    pub(crate) refresh: Refresh,
}

impl Validated {
    pub(crate) fn try_new(config: Config) -> Result<Self, Error> {
        use o3::base::time::Zero;

        let servers = Configured::try_new(config.servers)?;
        let attempt = Positive::try_new(config.query.attempt_timeout)
            .map_err(|Zero| Error::QueryAttemptTimeoutZero)?;
        let total = Positive::try_new(config.query.total_timeout)
            .map_err(|Zero| Error::QueryTotalTimeoutZero)?;
        if total < attempt {
            return Err(Error::QueryTotalTimeoutShort { attempt, total });
        }
        let attempts = num::NonZero::new(config.query.attempts).ok_or(Error::QueryAttemptsZero)?;
        let refresh = config.refresh.validate()?;
        for server in config.servers.as_slice() {
            if config.bind.is_ipv4() != server.is_ipv4() {
                return Err(Error::AddressFamily {
                    bind: config.bind,
                    server: *server,
                });
            }
        }
        Ok(Self {
            bind: config.bind,
            servers,
            query: Query {
                attempt_timeout: attempt,
                total_timeout: total,
                attempts,
            },
            refresh,
        })
    }
}

impl Refresh {
    pub fn validate(self) -> Result<Self, Error> {
        if self.minimum.is_zero() {
            return Err(Error::RefreshMinimumZero);
        }
        if self.backoff.base.is_zero() {
            return Err(Error::RefreshBackoffBaseZero);
        }
        if self.backoff.max < self.backoff.base {
            return Err(Error::RefreshBackoffOrder {
                base: self.backoff.base,
                maximum: self.backoff.max,
            });
        }
        if self.maximum_ttl.is_zero() {
            return Err(Error::RefreshMaximumTtlZero);
        }
        if self.minimum > self.maximum_ttl {
            return Err(Error::RefreshMinimumExceedsTtl {
                minimum: self.minimum,
                maximum: self.maximum_ttl,
            });
        }
        if self.before > self.maximum_ttl {
            return Err(Error::RefreshLeadExceedsTtl {
                before: self.before,
                maximum: self.maximum_ttl,
            });
        }
        Ok(self)
    }
}

const _: () = {
    assert!(mem::size_of::<Validated>() == mem::size_of::<Config>() + 16);
    assert!(mem::align_of::<Validated>() == mem::align_of::<Config>());
};
