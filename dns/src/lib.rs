//! Bounded DNS discovery driven by one dope application domain.

use std::{error, fmt, io, net, num};

use dope::runtime::random;

pub mod config;
pub mod discovery;
pub mod query;

mod name;
mod storage;
pub mod transport;
pub mod wire;

pub use name::Name;
pub use storage::{
    Storage,
    resolver::Resolver,
    timestamp::{Timestamp, TimestampError},
};
pub(crate) use storage::{addresses::Addresses, failure::Failure};

pub const HASH_DOMAIN: random::Domain = random::Domain::from_bytes(*b"\0\0\0\0\0dns");

#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::large_enum_variant,
    reason = "a thread-local target owns its bounded name without allocation or shared ownership"
)]
pub(crate) enum Host {
    Name(Name),
    Ip(net::IpAddr),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    host: Host,
    port: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetRef<'a> {
    host: HostRef<'a>,
    port: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostRef<'a> {
    Name(&'a Name),
    Ip(net::IpAddr),
}

#[derive(Debug)]
struct InvalidPort(num::ParseIntError);

impl fmt::Display for InvalidPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid service target port: {}", self.0)
    }
}

impl error::Error for InvalidPort {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.0)
    }
}

impl Target {
    pub fn parse(input: &str) -> io::Result<Self> {
        let (host, port) = if let Some(rest) = input.strip_prefix('[') {
            let Some((host, port)) = rest.split_once("]:") else {
                return Err(Self::invalid("bracketed IPv6 target must include a port"));
            };
            (host, port)
        } else {
            input
                .rsplit_once(':')
                .ok_or_else(|| Self::invalid("service target must be host:port"))?
        };
        if host.is_empty() {
            return Err(Self::invalid("service target host is empty"));
        }
        let port = port
            .parse::<u16>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, InvalidPort(error)))?;
        if port == 0 {
            return Err(Self::invalid("service target port must be nonzero"));
        }
        let host = match host.parse::<net::IpAddr>() {
            Ok(ip) => Host::Ip(ip),
            Err(_) => Host::Name(Name::parse(host)?),
        };
        Ok(Self { host, port })
    }

    pub const fn port(&self) -> u16 {
        self.port
    }

    pub fn name(&self) -> Option<&Name> {
        self.as_ref().name()
    }

    pub fn ip(&self) -> Option<net::IpAddr> {
        self.as_ref().ip()
    }

    pub fn as_ref(&self) -> TargetRef<'_> {
        let host = match &self.host {
            Host::Name(name) => HostRef::Name(name),
            Host::Ip(ip) => HostRef::Ip(*ip),
        };
        TargetRef {
            host,
            port: self.port,
        }
    }

    pub(crate) fn into_parts(self) -> (Host, u16) {
        (self.host, self.port)
    }

    fn invalid(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, message)
    }
}

impl<'a> TargetRef<'a> {
    pub const fn port(self) -> u16 {
        self.port
    }

    pub fn name(self) -> Option<&'a Name> {
        match self.host {
            HostRef::Name(name) => Some(name),
            HostRef::Ip(_) => None,
        }
    }

    pub fn ip(self) -> Option<net::IpAddr> {
        match self.host {
            HostRef::Name(_) => None,
            HostRef::Ip(ip) => Some(ip),
        }
    }

    pub(crate) const fn named(name: &'a Name, port: u16) -> Self {
        Self {
            host: HostRef::Name(name),
            port,
        }
    }

    pub(crate) const fn literal(ip: net::IpAddr, port: u16) -> Self {
        Self {
            host: HostRef::Ip(ip),
            port,
        }
    }
}

impl fmt::Display for Target {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.host {
            Host::Name(name) => write!(formatter, "{name}:{}", self.port),
            Host::Ip(net::IpAddr::V4(ip)) => write!(formatter, "{ip}:{}", self.port),
            Host::Ip(net::IpAddr::V6(ip)) => write!(formatter, "[{ip}]:{}", self.port),
        }
    }
}

impl fmt::Display for TargetRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.host {
            HostRef::Name(name) => write!(formatter, "{name}:{}", self.port),
            HostRef::Ip(net::IpAddr::V4(ip)) => write!(formatter, "{ip}:{}", self.port),
            HostRef::Ip(net::IpAddr::V6(ip)) => write!(formatter, "[{ip}]:{}", self.port),
        }
    }
}

impl From<TargetRef<'_>> for Target {
    fn from(target: TargetRef<'_>) -> Self {
        let host = match target.host {
            HostRef::Name(name) => Host::Name(name.clone()),
            HostRef::Ip(ip) => Host::Ip(ip),
        };
        Self {
            host,
            port: target.port,
        }
    }
}

impl From<net::SocketAddr> for Target {
    fn from(address: net::SocketAddr) -> Self {
        Self {
            host: Host::Ip(address.ip()),
            port: address.port(),
        }
    }
}
