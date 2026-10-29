use std::{marker, mem, time};

use dope_core::{
    driver,
    driver::{
        route::{Identity, KeyTag},
        schedule::clock,
        settings,
        settings::{claim, demands, errors},
    },
};

use crate::{
    app::{creation, dispatch},
    timing::timer,
};

const PERIOD: time::Duration = time::Duration::from_secs(1);

pub struct Component<ID: Identity> {
    route: marker::PhantomData<fn() -> ID>,
}

impl<ID: Identity> Component<ID> {
    pub const fn every_second() -> Self {
        Self {
            route: marker::PhantomData,
        }
    }
}

impl<ID: Identity> settings::Resource for Component<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(
            claim::Claim::new(settings::Routes::singleton::<ID>())
                .schedule(demands::Schedule::fixed::<1, 1>()),
        )
    }
}

/// A generation-safe, cadence-preserving one-second timer route.
#[pin_project::pin_project]
pub struct Interval<'d, ID: Identity> {
    #[pin]
    route: timer::Timer<'d, ID>,
    cadence: Cadence<'d>,
    tick: Tick,
}

#[derive(Clone, Copy)]
enum Cadence<'d> {
    Starting,
    Next(clock::Deadline<'d>),
    Stopped,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Tick {
    Empty,
    Ready,
}

/// Lifecycle-preserving commands available during one application step.
#[repr(transparent)]
pub struct Control<'step, 'd, ID: Identity>
where
    'd: 'step,
{
    inner: dispatch::raw::Installed<'step, 'd, Interval<'d, ID>>,
}

impl<'step, 'd, ID: Identity> Control<'step, 'd, ID>
where
    'd: 'step,
{
    /// Consumes the coalesced tick recorded by the latest activation.
    pub fn take_tick(&mut self) -> Tick {
        let tick = self.inner.as_mut().project().tick;
        mem::replace(tick, Tick::Empty)
    }
}

impl<'d, ID: Identity> Interval<'d, ID> {
    pub fn every_second(driver: &mut driver::Context<'_, 'd>) -> Result<Self, creation::Error> {
        Ok(Self {
            route: timer::Timer::new(driver)?,
            cadence: Cadence::Starting,
            tick: Tick::Empty,
        })
    }

    fn next(previous: clock::Deadline<'d>, now: clock::Moment<'d>) -> Option<clock::Deadline<'d>> {
        let next = previous.clamp_add(PERIOD);
        if next > now {
            return Some(next);
        }

        let skipped = now.past_due(next).as_secs().saturating_add(1);
        let next = next.clamp_add(time::Duration::from_secs(skipped));
        (next > now).then_some(next)
    }
}

const _: () = {
    assert!(mem::size_of::<Component<KeyTag<0>>>() == 0);
    assert!(mem::size_of::<Tick>() == 1);
};

mod raw;
