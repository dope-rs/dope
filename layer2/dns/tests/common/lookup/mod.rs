use dope::{core::driver::route::KeyTag, runtime::executor::session::Completion};
pub mod cases;
pub mod failure;
pub mod server;

use std::{net::SocketAddr, task::Poll, time::Duration};

use dope::{
    manifold::service::discover::{self, PollAt as _, RefreshAt as _},
    runtime,
};
use dope_dns::{
    Report, Resolver, config,
    lookup::{self, create, error::Error, refresh::Transport},
    storage,
};
use dope_fiber::{abi, context};
use dope_test::{alloc::Tracker, fibers, harness::scenario::TIMEOUT};
use failure::{App, Run};
use runtime::topology::lending;

use crate::common::fixture;

pub fn configure(
    servers: config::Servers,
    tries: u8,
    attempt: Duration,
    total: Duration,
) -> config::Config {
    let mut config = fixture::configure(servers, tries);
    config.query.attempt_timeout = attempt;
    config.query.total_timeout = total;
    config
}

struct Resolve<'app, 's, 'd, B, const LANES: usize> {
    query: lookup::Discovery<'app, 's, 'd, Transport, LANES, 1, B>,
    start: bool,
}

impl<'app, 's, 'd, B: Unpin, const LANES: usize> abi::Fiber<'d>
    for Resolve<'app, 's, 'd, B, LANES>
{
    type Output = Result<Result<Option<SocketAddr>, Error<1>>, Run>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut cx) = call.into_parts();
        let this = this.get_mut();
        let now = cx.as_mut().driver().driver_ref().clock().now();
        let (action, count) = Tracker::count(|| {
            if !this.start {
                this.query.refresh_at(now, discover::Reason::Startup);
                this.start = true;
            }
            let action = this.query.poll_at(cx.as_mut().driver().state_token(), now);
            if matches!(action, discover::Action::Pending { .. })
                && !matches!(
                    this.query.poll_at(cx.as_mut().driver().state_token(), now),
                    discover::Action::Pending { .. }
                )
            {
                return Err(Run::Repeated);
            }
            Ok(action)
        });
        if count != 0 {
            return Poll::Ready(Err(Run::Allocated(count)));
        }
        let action = match action {
            Ok(action) => action,
            Err(error) => return Poll::Ready(Err(error)),
        };
        match action {
            discover::Action::Pending { .. }
            | discover::Action::Stale { .. }
            | discover::Action::Expired { .. } => {}
            discover::Action::Published { snapshot, .. } => {
                return Poll::Ready(Ok(Ok(snapshot.into_iter().next().map(|item| *item.id()))));
            }
            discover::Action::Failed { error, .. } => return Poll::Ready(Ok(Err(error))),
        }
        cx.wake();
        Poll::Pending
    }
}

pub struct Query<'report, 'a, const LANES: usize = 1>(pub &'a str, pub &'report mut Completion);

impl<
    'report,
    'stage,
    'q,
    'h,
    'p,
    'scope,
    'life: 'scope,
    'e: 'life,
    'd: 'e,
    R: Report,
    const LANES: usize,
>
    lending::Next<
        lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::Plan<LANES, 1>, ()>,
        Resolver<'q, 'h, 'p, 'life, 'e, 'd, KeyTag<0>, KeyTag<1>, LANES, 1, R>,
    > for Query<'report, '_, LANES>
{
    type Output = Result<Result<Option<SocketAddr>, Error<1>>, App<'report>>;

    fn run(
        self,
        resolver: Resolver<'q, 'h, 'p, 'life, 'e, 'd, KeyTag<0>, KeyTag<1>, LANES, 1, R>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, storage::Plan<LANES, 1>, ()>,
    ) -> Self::Output {
        let now = (LANES == 2).then(|| stage.driver().driver_ref().clock().now());
        stage
            .with_quiesce(
                self.1,
                resolver,
                TIMEOUT,
                |mut app| -> Result<Result<Option<SocketAddr>, Error<1>>, Run> {
                    let resolver = app.root_client();
                    let target = dope_dns::Target::parse(self.0).map_err(Run::Target)?;
                    let spare = target.try_clone().map_err(Run::Exhausted)?;
                    let mut lane = resolver
                        .discovery::<Transport>(app.state_token(), spare)
                        .map_err(Run::Create)?;
                    if lane.target() != target.as_ref() {
                        return Err(Run::TargetClone);
                    }
                    let retained = if let Some(now) = now {
                        let second = target.try_clone().map_err(Run::Exhausted)?;
                        let mut second = resolver
                            .discovery::<Transport>(app.state_token(), second)
                            .map_err(Run::Create)?;
                        let overflow = target.try_clone().map_err(Run::Exhausted)?;
                        match resolver.discovery::<Transport>(app.state_token(), overflow) {
                            Err(create::Error::LaneCapacity {
                                target: rejected,
                                capacity,
                            }) if capacity == LANES && rejected.as_ref() == target.as_ref() => {}
                            Err(error) => return Err(Run::Create(error)),
                            Ok(_) => return Err(Run::LaneAdmission),
                        }
                        for query in [&mut lane, &mut second] {
                            query.refresh_at(now, discover::Reason::Startup);
                            if !matches!(
                                query.poll_at(app.state_token(), now),
                                discover::Action::Pending { .. }
                            ) {
                                return Err(Run::LaneAdmission);
                            }
                        }
                        Some(second)
                    } else {
                        None
                    };
                    drop(lane);
                    let query = resolver
                        .discovery::<Transport>(app.state_token(), target)
                        .map_err(Run::Create)?;
                    let start = retained.is_some();
                    let (query, _held) = match retained {
                        Some(retained) => (retained, Some(query)),
                        None => (query, None),
                    };
                    fibers::TEST
                        .try_run(&mut app, Resolve { query, start })
                        .map_err(Run::Fiber)?
                },
            )
            .map_err(App::Session)?
            .map_err(App::Run)
    }
}
