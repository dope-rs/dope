use std::{cell::Cell, rc::Rc, time::Duration};

use client::side::{self};
use dope_core::driver::{
    schedule::ready,
    settings,
    settings::{claim, errors},
    storage,
};
use dope_manifold::client;
use dope_runtime::{
    executor,
    executor::session::{Completion, Error},
    topology::lending,
};
use dope_test::{
    fibers,
    harness::{observe::Trace, signals::LocalGateRef},
};
use o3::buffer::storage::Shared;

use crate::common::{
    contract::Report,
    manifold::client::{
        error,
        side::{Reservation, Session, State, Storage},
    },
};

pub struct App<'test, F, G> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) create: F,
    pub(crate) ready: G,
    pub(crate) conn: LocalGateRef<'test>,
    pub(crate) done: LocalGateRef<'test>,
    pub(crate) closed: LocalGateRef<'test>,
    pub(crate) events: &'test Trace<Result<(), side::Error>, 4>,
    pub(crate) eager: bool,
}

impl<F, G> settings::Resource for App<'_, F, G> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<'test, F: Fn() -> Shared, G: for<'d> Fn(Option<ready::Target<'d>>) -> ready::Target<'d>>
    App<'test, F, G>
{
    pub fn build<'life, 'p, 'd>(
        self,
        registry: &'life <Storage as storage::Plan>::Output<'p, 'd>,
        claims: Rc<Trace<Reservation<'d>, 2>>,
    ) -> Session<'test, 'life, 'd, F, G>
    where
        'd: 'p,
        'p: 'life,
        Self: 'd,
    {
        Session {
            create: self.create,
            ready: self.ready,
            audit: self.audit,
            claims,

            state: State {
                pending: Cell::new(None),
                eager: self.eager,
                armed: Cell::new(false),
                done: self.done,
                closed: self.closed,
                events: self.events,
            },
            registry,
            conn: self.conn,
        }
    }
}

pub struct Wait<'report, 'a, 'b> {
    pub(crate) finish: &'report mut Completion,
    pub(crate) conn: LocalGateRef<'a>,
    pub(crate) done: LocalGateRef<'b>,
}

impl<'report, 'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, A>
    lending::Next<lending::Stage<'stage, 'scope, 'life, 'p, 'd, Storage, ()>, A>
    for Wait<'report, '_, '_>
where
    A: executor::raw::App<'d>,
{
    type Output = Result<
        Result<(fibers::Wait, Option<fibers::Wait>), fibers::Run>,
        Error<'report, Result<(fibers::Wait, Option<fibers::Wait>), fibers::Run>>,
    >;

    fn run(
        self,
        host: A,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, Storage, ()>,
    ) -> Self::Output {
        stage.with_quiesce(self.finish, host, Duration::from_secs(2), |mut app| {
            let first = fibers::TEST.try_wait(&mut app, &self.conn, 1)?;
            let done = match first {
                fibers::Wait::Reached => Some(fibers::TEST.try_wait(&mut app, &self.done, 1)?),
                fibers::Wait::Deadline => None,
            };
            Ok((first, done))
        })
    }
}
