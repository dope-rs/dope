use std::{ops::ControlFlow, task, time::Duration};

use dope_core::{
    driver,
    driver::{
        result::{
            poll,
            poll::{Dispatch, fault},
        },
        schedule::state::scope,
    },
    io::event::{
        Kind,
        net::{connect, tuning},
    },
};

use crate::common::driver::polling::{Deadline, Error, Source};

pub enum Failure<'d> {
    Wait(poll::Error),
    Dispatch(fault::Dispatch),
    Event(Kind<'d>),
}

struct Pending<'a, 'context, 'turn, 'd> {
    driver: &'a mut driver::Context<'context, 'd>,
    turn: &'a mut scope::Guard<'turn, 'd>,
}

impl<'d> Source for Pending<'_, '_, '_, 'd> {
    type Output = Kind<'d>;
    type Error = Failure<'d>;

    fn try_poll(&mut self, left: Duration) -> Result<task::Poll<Self::Output>, Self::Error> {
        driver::Context::wait(self.driver, self.turn.reactor(), Some(left))
            .map_err(Failure::Wait)?;
        match driver::Context::dispatch(self.driver, self.turn.reactor(), |event, _driver| {
            ControlFlow::Break(event)
        }) {
            Dispatch::Ready { retained: None, .. } => Ok(task::Poll::Pending),
            Dispatch::Ready {
                retained: Some(event),
                ..
            } => Ok(task::Poll::Ready(event.into_kind())),
            Dispatch::Fault(error) => Err(Failure::Dispatch(error)),
        }
    }
}

pub trait Tuned<'d> {
    fn wait_tuning(
        &mut self,
        turn: &mut scope::Guard<'_, 'd>,
    ) -> Result<tuning::Completion<'d>, Error<Failure<'d>>>;

    fn wait_connect(
        &mut self,
        turn: &mut scope::Guard<'_, 'd>,
    ) -> Result<connect::Completion<'d>, Error<Failure<'d>>>;
}

impl<'d> Tuned<'d> for driver::Context<'_, 'd> {
    fn wait_tuning(
        &mut self,
        turn: &mut scope::Guard<'_, 'd>,
    ) -> Result<tuning::Completion<'d>, Error<Failure<'d>>> {
        match Deadline::new(Duration::from_secs(1)).try_run(Pending { driver: self, turn })? {
            Kind::Tuning(done) => Ok(done),
            other => Err(Error::Source(Failure::Event(other))),
        }
    }

    fn wait_connect(
        &mut self,
        turn: &mut scope::Guard<'_, 'd>,
    ) -> Result<connect::Completion<'d>, Error<Failure<'d>>> {
        match Deadline::new(Duration::from_secs(1)).try_run(Pending { driver: self, turn })? {
            Kind::Connect(done) => Ok(done),
            other => Err(Error::Source(Failure::Event(other))),
        }
    }
}
