use std::{io, pin, time};

use dope_core::driver;

use crate::timing::timer;

mod sealed;

const PERIOD: time::Duration = time::Duration::from_secs(1);

/// A generation-safe, cadence-preserving one-second timer route.
#[pin_project::pin_project]
pub struct Interval<'d, const ID: u8> {
    #[pin]
    route: timer::Timer<'d, ID>,
    next: Option<time::Instant>,
    tick: bool,
    stopped: bool,
}

/// Lifecycle-preserving commands available during one application step.
pub struct Control<'step, 'd, const ID: u8>
where
    'd: 'step,
{
    inner: pin::Pin<&'step mut Interval<'d, ID>>,
}

impl<'step, 'd, const ID: u8> Control<'step, 'd, ID>
where
    'd: 'step,
{
    /// Consumes the coalesced tick recorded by the latest activation.
    pub fn take_tick(&mut self) -> bool {
        let tick = self.inner.as_mut().project().tick;
        if *tick {
            *tick = false;
            true
        } else {
            false
        }
    }
}

impl<'d, const ID: u8> Interval<'d, ID> {
    pub fn every_second(driver: &mut driver::Context<'_, 'd>) -> io::Result<Self> {
        Ok(Self {
            route: timer::Timer::new(driver)?,
            next: None,
            tick: false,
            stopped: false,
        })
    }

    fn following_deadline(previous: time::Instant, now: time::Instant) -> Option<time::Instant> {
        let next = Self::deadline_after(previous, PERIOD)?;
        if next > now {
            return Some(next);
        }

        let skipped = now.duration_since(next).as_secs().checked_add(1)?;
        Self::deadline_after(next, time::Duration::from_secs(skipped))
    }

    fn deadline_after(at: time::Instant, elapsed: time::Duration) -> Option<time::Instant> {
        at.checked_add(elapsed)
    }
}
