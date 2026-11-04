use std::task::Poll;

use dope_core::{
    driver::route::Identity,
    io::{Event, socket::install::Install},
};
use o3::alloc;

use crate::common::{contract::submit, driver::polling::Error, file::Inspect};
pub mod accept;
pub mod support;
pub mod task;
pub mod tuning;

use std::{net::SocketAddr, ops::ControlFlow, time::Duration};

use dope_core::{
    driver,
    driver::{life, result::poll, retained::flight, route, schedule::state::scope::Guard},
    io::{
        fd::handles,
        socket::{install, install::slot},
    },
};

pub enum Failure<'d> {
    Bind(install::Error),
    Wait(poll::Error),
    Dispatch(poll::Dispatch<'d>),
}

#[derive(Debug)]
pub enum Cause<'a> {
    Bind(&'a install::Error),
    Wait(&'a poll::Error),
    Dispatch(&'a poll::Failure),
    Retained {
        drain: poll::Drain,
        route: Option<route::Route>,
    },
}

impl Failure<'_> {
    pub fn cause(&self) -> Cause<'_> {
        match self {
            Self::Bind(error) => Cause::Bind(error),
            Self::Wait(error) => Cause::Wait(error),
            Self::Dispatch(poll::Dispatch::Fault(error)) => Cause::Dispatch(error),
            Self::Dispatch(poll::Dispatch::Ready { drain, retained }) => Cause::Retained {
                drain: *drain,
                route: retained.as_ref().map(Event::route),
            },
        }
    }
}

pub struct Wait<'a, 'context, 'turn, 'd> {
    pub driver: &'a mut driver::Context<'context, 'd>,
    pub turn: &'a mut Guard<'turn, 'd>,
    pub timeout: Duration,
}

impl<'d> Wait<'_, '_, '_, 'd> {
    pub fn try_run(self) -> Result<(), Failure<'d>> {
        driver::Context::wait(self.driver, self.turn.reactor(), Some(self.timeout))
            .map_err(Failure::Wait)?;
        let outcome =
            driver::Context::dispatch(self.driver, self.turn.reactor(), |event, _source| {
                ControlFlow::Break(event)
            });
        match outcome {
            poll::Dispatch::Ready { retained: None, .. } => Ok(()),
            other => Err(Failure::Dispatch(other)),
        }
    }
}

pub struct Open;

impl Inspect for Open {
    fn contains(&self, fd: libc::c_int) -> bool {
        // SAFETY: F_GETFD only queries the integer descriptor and writes no memory.
        unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
    }
}

pub trait Scoped<'d> {
    fn run_turn<R>(
        &mut self,
        op: impl for<'context, 'turn> FnOnce(driver::Context<'context, 'd>, &mut Guard<'turn, 'd>) -> R,
    ) -> R;
    fn with_flights<ID: Identity, const N: usize>(
        &mut self,
        f: impl for<'a, 'turn> FnOnce(
            driver::Context<'a, 'd>,
            &mut Guard<'turn, 'd>,
            flight::Slots<'d, ID>,
        ),
    ) -> Result<(), alloc::Error>;
}
impl<'d> Scoped<'d> for life::Scope<'_, 'd> {
    fn run_turn<R>(
        &mut self,
        op: impl for<'context, 'turn> FnOnce(driver::Context<'context, 'd>, &mut Guard<'turn, 'd>) -> R,
    ) -> R {
        self.with_driver(|context, mut schedule| {
            let mut turn = schedule.begin();
            op(context, &mut turn)
        })
    }
    fn with_flights<ID: Identity, const N: usize>(
        &mut self,
        f: impl for<'a, 'turn> FnOnce(
            driver::Context<'a, 'd>,
            &mut Guard<'turn, 'd>,
            flight::Slots<'d, ID>,
        ),
    ) -> Result<(), alloc::Error> {
        self.run_turn(|mut context, turn| {
            let slots = (context
                .storage
                .reborrow()
                .try_flights(flight::Need::<ID>::new(flight::Capacity::fixed::<N>())))?;
            let slots = slots.slots();
            f(context, turn, slots);
            Ok(())
        })
    }
}

/// Native fixture operations preserving the borrowed driver and turn.
pub trait Native<'d> {
    fn rebind(
        &mut self,
        turn: &mut Guard<'_, 'd>,
    ) -> Result<(handles::Datagram<'d>, SocketAddr), Error<Failure<'d>>>;
    fn submit_recv<ID: Identity>(
        &mut self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, ID>,
    ) -> Result<(), submit::Error>;
}

impl<'d> Native<'d> for driver::Context<'_, 'd> {
    fn rebind(
        &mut self,
        turn: &mut Guard<'_, 'd>,
    ) -> Result<(handles::Datagram<'d>, SocketAddr), Error<Failure<'d>>> {
        polling::try_run(Duration::from_secs(1), |left| {
            match Install::bind_udp(
                self.reborrow(),
                ([127, 0, 0, 1], 0).into(),
                crate::common::DATAGRAM,
            ) {
                Err(install::Error::Slot(slot::Error::Retiring { available: 0 })) => {}
                Ok(output) => return Ok(Poll::Ready(output)),
                Err(error) => return Err(Failure::Bind(error)),
            }
            Wait {
                driver: self,
                turn,
                timeout: left,
            }
            .try_run()?;
            Ok(Poll::Pending)
        })
    }

    fn submit_recv<ID: Identity>(
        &mut self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, ID>,
    ) -> Result<(), submit::Error> {
        let slots = self
            .storage
            .reborrow()
            .try_flights(flight::Need::<ID>::new(flight::Capacity::fixed::<1>()))
            .map_err(submit::Error::Allocation)?
            .slots();
        let flight = slots
            .recv(self.reborrow(), fd, target)
            .map_err(submit::Error::Submit)?;
        drop(flight);
        Ok(())
    }
}

pub mod polling;
