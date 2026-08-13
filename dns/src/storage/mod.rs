use std::io;

use dope::{core::driver, manifold::service::attach, net::link::egress, runtime::random};

use crate::{config, discovery, storage::state::indices};

pub(crate) mod addresses;
pub(crate) mod failure;
pub(crate) mod queries;
pub(crate) mod resolver;
pub(crate) mod state;
pub(crate) mod timestamp;

/// Fixed resolver state for one application domain.
///
/// ```compile_fail,E0599
/// use dope_dns::{Storage, Target};
/// fn bypass(storage: &Storage<1, 4>, target: Target) {
///     let _ = storage.discovery(target);
/// }
/// ```
///
/// The address capacity is a compile-time service bound.
///
/// ```compile_fail,E0080
/// use std::{net, time};
/// use dope_dns::{Storage, config, query::{self, family, server}, transport};
/// fn config() -> config::Config {
///     let second = time::Duration::from_secs(1);
///     config::Config {
///         policy: query::Policy::new(
///             transport::DatagramThenStream,
///             server::Cycle,
///             family::RequireAll,
///         ),
///         bind: net::SocketAddr::from(([127, 0, 0, 1], 0)),
///         servers: config::Servers::empty(),
///         query: config::Query { timeout: second, attempts: 1 },
///         refresh: config::Refresh {
///             minimum: second,
///             backoff: config::Backoff { base: second, max: second },
///             maximum_ttl: second,
///             before: time::Duration::ZERO,
///         },
///     }
/// }
/// let _ = Storage::<1, 0>::new(config());
/// ```
///
/// ```compile_fail,E0080
/// use std::{net, time};
/// use dope::manifold::service;
/// use dope_dns::{Storage, config, query::{self, family, server}, transport};
/// fn config() -> config::Config {
///     let second = time::Duration::from_secs(1);
///     config::Config {
///         policy: query::Policy::new(
///             transport::DatagramThenStream,
///             server::Cycle,
///             family::RequireAll,
///         ),
///         bind: net::SocketAddr::from(([127, 0, 0, 1], 0)),
///         servers: config::Servers::empty(),
///         query: config::Query { timeout: second, attempts: 1 },
///         refresh: config::Refresh {
///             minimum: second,
///             backoff: config::Backoff { base: second, max: second },
///             maximum_ttl: second,
///             before: time::Duration::ZERO,
///         },
///     }
/// }
/// let _ = Storage::<1, { service::MAX_ENDPOINTS + 1 }>::new(config());
/// ```
///
/// A discovery cannot outlive the storage that owns its lane.
///
/// ```compile_fail,E0515
/// use dope_dns::{Storage, Target, config, discovery::Discovery};
/// fn escape(config: config::Config, target: Target) -> Discovery<'static, 1, 4> {
///     let storage = Storage::<1, 4>::new(config).unwrap();
///     storage.literal(target).unwrap()
/// }
/// ```
pub struct Storage<const LANES: usize, const ADDRESSES: usize> {
    datagram: attach::Attach,
    stream: attach::Attach,
    pub(crate) queries: queries::Queries<LANES, ADDRESSES>,
}

impl<const LANES: usize, const ADDRESSES: usize> Storage<LANES, ADDRESSES> {
    pub fn new(config: config::Config) -> io::Result<Self> {
        let () = addresses::AddressCount::<ADDRESSES>::VALID;
        config.validate()?;
        let indices = indices::IndexLayout::new(LANES)?;
        Ok(Self {
            datagram: attach::Attach::new(),
            stream: attach::Attach::new(),
            queries: queries::Queries::try_new(config, indices)?,
        })
    }

    pub const fn config(&self) -> config::Config {
        self.queries.config
    }

    pub(crate) const fn config_ref(&self) -> &config::Config {
        &self.queries.config
    }

    pub fn bind<'d, const DATAGRAM_ID: u8, const STREAM_ID: u8, const OUTBOUND: usize>(
        &'d self,
        seed: random::HashState<'d>,
        egress: egress::Config,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<resolver::Resolver<'d, DATAGRAM_ID, STREAM_ID, LANES, ADDRESSES>> {
        use crate::transport::{datagram, stream};

        if DATAGRAM_ID == STREAM_ID {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS datagram and stream routes must be distinct",
            ));
        }
        if self.config_ref().servers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS resolver requires at least one explicit server",
            ));
        }
        let datagram_plan = datagram::Plan::new(OUTBOUND)?;
        let datagram = self
            .datagram
            .bind(self)
            .map_err(|error| io::Error::new(io::ErrorKind::AlreadyExists, error))?;
        let stream = self
            .stream
            .bind(self)
            .map_err(|error| io::Error::new(io::ErrorKind::AlreadyExists, error))?;
        {
            use std::hash;
            self.queries
                .seed(hash::BuildHasher::hash_one(&seed, "dope::dns"));
        }
        let datagram = datagram_plan.bind(datagram, self.config_ref().bind, driver)?;
        let stream = stream::Stream::new(stream, self, seed, egress, driver)?;
        Ok(resolver::Resolver::new(self, datagram, stream))
    }

    pub fn literal(
        &self,
        target: crate::Target,
    ) -> io::Result<discovery::Discovery<'_, LANES, ADDRESSES>> {
        let (host, port) = target.into_parts();
        let crate::Host::Ip(ip) = host else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "hostname discovery must be issued by a bound Resolver",
            ));
        };
        Ok(discovery::Discovery::literal(
            ip,
            port,
            &self.config_ref().refresh,
        ))
    }

    pub(crate) fn discovery_bound(
        &self,
        target: crate::Target,
    ) -> io::Result<discovery::Discovery<'_, LANES, ADDRESSES>> {
        let (host, port) = target.into_parts();
        match host {
            crate::Host::Ip(ip) => Ok(discovery::Discovery::literal(
                ip,
                port,
                &self.config_ref().refresh,
            )),
            crate::Host::Name(name) => {
                let lane = self.queries.claim()?;
                Ok(discovery::Discovery::query(
                    name,
                    port,
                    lane,
                    &self.config_ref().refresh,
                ))
            }
        }
    }
}
