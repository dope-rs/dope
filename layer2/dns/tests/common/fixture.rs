use std::{net, time};

use dope::{
    core::{
        Errno,
        driver::{
            result::poll::Close,
            route::{self, KeyTag, kind::Kind},
            settings::errors,
        },
        io::{Reclaimed, datagram::ReceiveError, event::batch::Flow},
    },
    manifold::timing::Balanced,
    runtime::{
        executor::session,
        topology::{self, lending},
    },
};
use dope_dns::{
    Resolver, config, storage,
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
        Reclaimed::Shutdown
        | Reclaimed::Close {
            file: Close::Closed(Ok(())),
            reactor: Ok(()),
            ..
        } => Ok(()),
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

pub struct QueryDeadline<'a>(pub &'a mut session::Completion);

impl<'report, 'stage, 'q, 'h, 'p, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, R: dope_dns::Report>
    lending::Next<
        lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::Plan<1, 4>, ()>,
        Resolver<'q, 'h, 'p, 'life, 'e, 'd, KeyTag<0>, KeyTag<1>, 1, 4, R>,
    > for QueryDeadline<'report>
{
    type Output = ();

    fn run(
        self,
        resolver: Resolver<'q, 'h, 'p, 'life, 'e, 'd, KeyTag<0>, KeyTag<1>, 1, 4, R>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::Plan<1, 4>, ()>,
    ) {
        use dope::manifold::service::discover::{self, Action, PollAt as _, RefreshAt as _};
        use dope_dns::{
            Target,
            lookup::{
                error::{Error, Kind},
                refresh::Transport,
            },
        };
        use dope_test::harness::scenario::TIMEOUT;
        use o3::base::time::Positive;

        let now = stage.driver().driver_ref().clock().now();
        let range = now
            .try_add(Positive::try_new(time::Duration::MAX).expect("positive total"))
            .expect_err("query exceeds the clock range");
        stage
            .with_quiesce(self.0, resolver, TIMEOUT, |mut app| {
                let target = Target::parse("api.example:443").expect("named target");
                let resolver = app.root_client();
                let mut lookup = resolver
                    .discovery::<Transport>(app.state_token(), target)
                    .expect("query lane");
                lookup.refresh_at(now, discover::Reason::Startup);
                assert!(matches!(
                    lookup.poll_at(app.state_token(), now),
                    Action::Failed {
                        error: Error::Query { kind: Kind::Deadline(source), .. },
                        poll_at: None,
                    } if source == range
                ));
                lookup.refresh_at(now, discover::Reason::Startup);
                assert!(matches!(
                    lookup.poll_at(app.state_token(), now),
                    Action::Pending { poll_at: None }
                ));
            })
            .expect("DNS shutdown");
    }
}
