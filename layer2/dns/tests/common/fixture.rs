use std::{net, time};

use dope::{
    core::{
        Errno,
        driver::{
            route::{self, KeyTag, kind::Kind},
            settings::errors,
        },
        io::{Reclaimed, datagram::ReceiveError, event::batch::Flow},
    },
    manifold::timing::Balanced,
    runtime::topology,
};
use dope_dns::{
    config, storage,
    topology::{Plan, Prepare},
};

#[derive(Debug)]
pub enum Error {
    Storage(storage::Error),
    Resolver(Prepare),
    Claim(errors::Claim),
}

pub struct Received<'a>(&'a mut Vec<ReceiveError>);

impl dope_dns::Report for Received<'_> {
    fn receive(&mut self, cause: ReceiveError) {
        self.0.push(cause);
    }
}

pub fn try_prepare<const OUTBOUND: usize, const LANES: usize, const ADDRESSES: usize>(
    config: config::Config,
    received: &mut Vec<ReceiveError>,
) -> Result<
    topology::Prepared<
        storage::Plan<LANES, ADDRESSES>,
        Plan<KeyTag<0>, KeyTag<1>, OUTBOUND, LANES, ADDRESSES, Received<'_>>,
    >,
    Error,
> {
    let storage = storage::Plan::new(config).map_err(Error::Storage)?;
    let resolver = Plan::new(Default::default(), Received(received)).map_err(Error::Resolver)?;
    topology::Plan::new(storage, resolver)
        .for_install::<Balanced>()
        .map_err(Error::Claim)
}

pub(crate) fn shutdown(report: Reclaimed) -> Result<(), Reclaimed> {
    match report {
        Reclaimed::Shutdown => Ok(()),
        report => Err(report),
    }
}

pub fn reclaim(report: Reclaimed) -> Result<(), Reclaimed> {
    match report {
        Reclaimed::Recv {
            result: Err(Errno::CANCELED),
            flow: Flow::Last,
            ..
        } => Ok(()),
        Reclaimed::RecvRoute {
            target,
            result: Err(Errno::CANCELED),
            flow: Flow::Last,
        } if target.kind() == Ok(Kind::Recv)
            && [
                route::Id::fixed::<0>().into(),
                route::Id::fixed::<1>().into(),
            ]
            .contains(&target.route()) =>
        {
            Ok(())
        }
        report => shutdown(report),
    }
}

pub const SERVER: net::SocketAddr =
    net::SocketAddr::new(net::IpAddr::V4(net::Ipv4Addr::LOCALHOST), 53);

pub fn configure(servers: config::Servers, attempts: u8) -> config::Config {
    config::Config {
        bind: net::SocketAddr::from(([127, 0, 0, 1], 0)),
        servers,
        query: config::Query {
            attempt_timeout: time::Duration::from_secs(1),
            total_timeout: time::Duration::from_secs(u64::from(attempts.max(1))),
            attempts,
        },
        refresh: config::Refresh {
            minimum: time::Duration::from_millis(10),
            backoff: config::Backoff {
                base: time::Duration::from_millis(10),
                max: time::Duration::from_secs(1),
            },
            maximum_ttl: time::Duration::from_secs(60),
            before: time::Duration::from_secs(5),
            stale: time::Duration::from_secs(5),
        },
    }
}
