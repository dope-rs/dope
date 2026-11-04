use std::{ops::ControlFlow, task, time::Duration};

use dope_core::{
    driver,
    driver::{
        result::{poll, poll::Dispatch},
        schedule::state::scope,
    },
    io::event::{
        Kind,
        net::{connect, tuning},
    },
};

use crate::common::driver::polling::{self, Error};

pub enum Failure<'d> {
    Wait(poll::Error),
    Dispatch(poll::Failure),
    Event(Kind<'d>),
}

fn wait<'d>(
    driver: &mut driver::Context<'_, 'd>,
    turn: &mut scope::Guard<'_, 'd>,
) -> Result<Kind<'d>, Error<Failure<'d>>> {
    polling::try_run(Duration::from_secs(1), |left| {
        driver::Context::wait(driver, turn.reactor(), Some(left)).map_err(Failure::Wait)?;
        match driver::Context::dispatch(driver, turn.reactor(), |event, _driver| {
            ControlFlow::Break(event)
        }) {
            Dispatch::Ready { retained: None, .. } => Ok(task::Poll::Pending),
            Dispatch::Ready {
                retained: Some(event),
                ..
            } => Ok(task::Poll::Ready(event.into_kind())),
            Dispatch::Fault(error) => Err(Failure::Dispatch(error)),
        }
    })
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
        match wait(self, turn)? {
            Kind::Tuning(done) => Ok(done),
            other => Err(Error::Source(Failure::Event(other))),
        }
    }

    fn wait_connect(
        &mut self,
        turn: &mut scope::Guard<'_, 'd>,
    ) -> Result<connect::Completion<'d>, Error<Failure<'d>>> {
        match wait(self, turn)? {
            Kind::Connect(done) => Ok(done),
            other => Err(Error::Source(Failure::Event(other))),
        }
    }
}
