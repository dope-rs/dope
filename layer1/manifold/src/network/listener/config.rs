use dope_core::{driver::route::table::conn, io::socket};
use dope_net::{self, link::egress};

use crate::{ConfigError, app::env::quota};

/// Pinned direct-send provisioning resolved against final-link guarantees.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Flights {
    #[default]
    Minimum,
    Exact(usize),
    PerConnection,
}

impl From<usize> for Flights {
    fn from(value: usize) -> Self {
        Self::Exact(value)
    }
}

#[derive(Clone)]
pub struct Config<T: dope_net::Listen> {
    pub per_ip: Option<quota::PerIp>,
    pub max_connections: usize,
    /// Pinned direct-send provisioning. Queued slots are bounded by `egress`.
    pub direct_flights: Flights,
    pub bind: T::Addr,
    /// Requested kernel listen depth; the operating system may cap it to its host limit.
    pub backlog: socket::Backlog,
    pub stream: T::StreamConfig,
    pub transport: T::ListenerConfig,
    pub egress: egress::Config,
}

/// Listener configuration carrying its exact connection-capacity proof.
pub struct Prepared<T: dope_net::Listen> {
    pub per_ip: Option<quota::PerIp>,
    pub connections: conn::Capacity,
    pub direct_flights: Flights,
    pub bind: T::Addr,
    pub backlog: socket::Backlog,
    pub stream: T::StreamConfig,
    pub transport: T::ListenerConfig,
    pub egress: egress::Config,
}

impl<T: dope_net::Listen> Config<T> {
    pub fn try_prepare(self) -> Result<Prepared<T>, ConfigError> {
        let cap = conn::Capacity::try_from(self.max_connections)
            .map_err(ConfigError::ListenerCapacity)?;
        let Self {
            per_ip,
            max_connections: _,
            direct_flights: direct,
            bind,
            backlog,
            stream,
            transport: opts,
            egress,
        } = self;
        Ok(Prepared {
            per_ip,
            connections: cap,
            direct_flights: direct,
            bind,
            backlog,
            stream,
            transport: opts,
            egress,
        })
    }
}

impl<T: dope_net::Listen> Prepared<T> {
    pub const fn connections(&self) -> conn::Capacity {
        self.connections
    }
}

impl<T: dope_net::Listen> Clone for Prepared<T>
where
    T::Addr: Copy,
    T::ListenerConfig: Copy,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: dope_net::Listen> Copy for Prepared<T>
where
    T::Addr: Copy,
    T::ListenerConfig: Copy,
{
}
