use std::{io, net, time};

use crate::query;

pub(crate) const MAX_SERVERS: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Servers {
    values: [net::SocketAddr; MAX_SERVERS],
    len: u8,
}

impl Servers {
    pub const fn empty() -> Self {
        Self {
            values: [net::SocketAddr::new(net::IpAddr::V4(net::Ipv4Addr::UNSPECIFIED), 0);
                MAX_SERVERS],
            len: 0,
        }
    }

    pub fn try_from_iter(servers: impl IntoIterator<Item = net::SocketAddr>) -> io::Result<Self> {
        let mut result = Self::empty();
        for server in servers {
            if server.port() == 0 {
                return Err(invalid("DNS server port must be nonzero"));
            }
            let index = usize::from(result.len);
            if index == MAX_SERVERS {
                return Err(invalid("DNS server count exceeds the fixed bound"));
            }
            result.values[index] = server;
            result.len += 1;
        }
        Ok(result)
    }

    pub fn as_slice(&self) -> &[net::SocketAddr] {
        &self.values[..usize::from(self.len)]
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) const fn count(&self) -> u8 {
        self.len
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Query {
    pub timeout: time::Duration,
    pub attempts: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub base: time::Duration,
    pub max: time::Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct Refresh {
    pub minimum: time::Duration,
    pub backoff: Backoff,
    pub maximum_ttl: time::Duration,
    pub before: time::Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub policy: query::Policy,
    pub bind: net::SocketAddr,
    pub servers: Servers,
    pub query: Query,
    pub refresh: Refresh,
}

impl Config {
    pub fn validate(self) -> io::Result<Self> {
        if self.query.timeout.is_zero()
            || self.query.attempts == 0
            || self.refresh.minimum.is_zero()
            || self.refresh.backoff.base.is_zero()
            || self.refresh.backoff.max < self.refresh.backoff.base
            || self.refresh.maximum_ttl.is_zero()
            || self.refresh.minimum > self.refresh.maximum_ttl
            || self.refresh.before > self.refresh.maximum_ttl
        {
            return Err(invalid(
                "DNS durations and attempts must be nonzero and ordered",
            ));
        }
        let now = time::Instant::now();
        for duration in [
            self.query.timeout,
            self.refresh.minimum,
            self.refresh.backoff.max,
            self.refresh.maximum_ttl,
        ] {
            if now.checked_add(duration).is_none() {
                return Err(invalid("DNS duration exceeds the monotonic clock range"));
            }
        }
        for server in self.servers.as_slice() {
            if self.bind.is_ipv4() != server.is_ipv4() {
                return Err(invalid("DNS bind and server address families must match"));
            }
        }
        Ok(self)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
