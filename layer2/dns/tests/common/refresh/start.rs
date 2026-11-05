use std::{cell::Cell, rc::Rc, time::Duration};

use dope::{
    core::driver::{self, schedule::clock},
    manifold::service::discover::{self, PollAt as _, RefreshAt as _},
    runtime::{client, executor::session, topology::lending},
};
use dope_dns::{
    Report, Target,
    lookup::{create, refresh},
};
use dope_test::harness::{observe::VecCellExt as _, scenario::TIMEOUT};

use crate::common::{
    Owner,
    refresh::{Advance, App, Resolution, Run, Until, server},
};

pub(super) struct Start<'d> {
    pub target: Target,
    pub run: Run,
    pub server: server::Server,
    pub delay: Option<Duration>,
    pub now: clock::Moment<'d>,
}

pub(super) struct Control<'d> {
    run: Run,
    server: server::Server,
    delay: Option<Duration>,
    now: clock::Deadline<'d>,
    next: Rc<Cell<Option<clock::Deadline<'d>>>>,
}

impl<'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::Build<'d, Owner<'q, 'h, 'p, 'life, 's, 'd, R>> for Start<'d>
{
    type App<'app>
        = App<'app, 's, 'd, client::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>>>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app;
    type Data = Control<'d>;
    type Error = create::Error;

    fn build<'app>(
        self,
        resolver: <Owner<'q, 'h, 'p, 'life, 's, 'd, R> as client::Provider<'d>>::Client<'app>,
        mut driver: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app,
    {
        let mut query =
            resolver.discovery::<refresh::Transport>(driver.state_token(), self.target)?;
        query.refresh_at(self.now, discover::Reason::Startup);
        let data = Control {
            run: self.run.clone(),
            server: self.server,
            delay: self.delay,
            now: self.now.into(),
            next: Rc::new(Cell::new(Some(self.now.into()))),
        };
        let app = App {
            resolution: Resolution {
                discovery: query,
                run: self.run,
                next: data.next.clone(),
            },
        };
        Ok((app, data))
    }
}

impl<'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::ExecuteAt<'life, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, ()> for Start<'d>
{
    type Output = Result<(), Advance>;

    fn run<'app, 'run>(
        mut data: Self::Data,
        mut app: session::Manual<
            'run,
            'life,
            'd,
            lending::raw::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, Self::App<'app>>,
            (),
        >,
    ) -> Self::Output
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        while data.run.resolved.len() < data.run.stop_after {
            let fixed = app
                .drive_for(
                    data.now,
                    TIMEOUT,
                    Until::new(&data.run.changes, &mut data.server),
                )
                .map_err(Advance::Drive)?;
            if let Some(error) = data.run.fault.take() {
                return Err(error);
            }
            if let Some((reply, peer)) = fixed.into_output().map_err(Advance::Signal)? {
                let delay = data
                    .delay
                    .take()
                    .ok_or(Advance::Signal(server::Failure::Alias))?;
                data.now = data.now.try_add(delay).map_err(Advance::Clock)?;
                data.server.send(&reply, peer).map_err(Advance::Signal)?;
                continue;
            }
            if data.run.resolved.len() < data.run.stop_after {
                let due = data.next.get().ok_or(Advance::Deadline)?;
                if due <= data.now {
                    return Err(Advance::Deadline);
                }
                data.now = due;
            }
        }
        Ok(())
    }
}

pub struct Stop {
    pub target: Target,
    pub run: Run,
}

impl<'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::Build<'d, Owner<'q, 'h, 'p, 'life, 's, 'd, R>> for Stop
{
    type App<'app>
        = App<'app, 's, 'd, client::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>>>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app;
    type Data = ();
    type Error = create::Error;

    fn build<'app>(
        self,
        resolver: <Owner<'q, 'h, 'p, 'life, 's, 'd, R> as client::Provider<'d>>::Client<'app>,
        mut driver: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app,
    {
        let now = driver.driver_ref().clock().now();
        let mut query =
            resolver.discovery::<refresh::Transport>(driver.state_token(), self.target)?;
        query.refresh_at(now, discover::Reason::Startup);
        assert!(matches!(
            query.poll_at(driver.state_token(), now),
            discover::Action::Pending { .. }
        ));
        Ok((
            App {
                resolution: Resolution {
                    discovery: query,
                    run: self.run,
                    next: Rc::new(Cell::new(Some(now.into()))),
                },
            },
            (),
        ))
    }
}

impl<'q, 'h, 'p, 'life, 's, 'd: 'life, R: Report + 'd>
    lending::Execute<'life, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, ()> for Stop
{
    type Output = Result<(), Advance>;

    fn run<'app, 'run>(
        (): Self::Data,
        _: session::App<
            'run,
            'life,
            'd,
            lending::raw::Bound<'app, 'd, Owner<'q, 'h, 'p, 'life, 's, 'd, R>, Self::App<'app>>,
            (),
        >,
    ) -> Self::Output
    where
        Self: 'app,
        Owner<'q, 'h, 'p, 'life, 's, 'd, R>: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        Ok(())
    }
}
