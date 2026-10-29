//! Driver-bound civil-time authority for protocol transitions.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dope_core::driver::schedule::clock;
use o3::base::time;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    BeforeUnixEpoch { delta: Duration },
    NanosecondsOverflow { duration: Duration },
    Add { source: time::Add },
    Sub { source: time::Sub },
    MonotonicRegression { origin: Instant, observed: Instant },
}

impl From<time::Add> for Error {
    fn from(source: time::Add) -> Self {
        Self::Add { source }
    }
}

impl From<time::Sub> for Error {
    fn from(source: time::Sub) -> Self {
        Self::Sub { source }
    }
}

/// Statically dispatched source of absolute civil time.
pub trait Source: Sized {
    fn read(&self) -> Result<time::Unix<time::Nanos>, Error>;

    /// Pairs civil time with the monotonic sample immediately preceding its read.
    /// The clocks are sampled sequentially; their skew is confined to acquisition.
    /// Injected sources can supply the exact corresponding pair instead.
    fn sample(&self) -> Result<(time::Unix<time::Nanos>, Instant), Error> {
        let origin = Instant::now();
        Ok((self.read()?, origin))
    }

    /// Projects one fresh sample back to this admitted driver turn. Retaining the
    /// turn origin keeps observations in the same turn valid without counting
    /// time spent before acquisition twice in subsequent turns.
    fn anchor<'d>(&self, witness: &clock::Moment<'d>) -> Result<Anchor<'d>, Error> {
        let (epoch, observed) = self.sample()?;
        let origin = witness.into_raw();
        let elapsed = observed
            .checked_duration_since(origin)
            .ok_or(Error::MonotonicRegression { origin, observed })?;
        Ok(Anchor {
            epoch: epoch.try_sub(elapsed)?,
            origin: *witness,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub enum System {
    #[default]
    Wall,
}

impl System {
    pub const NEW: Self = Self::Wall;
}

impl Source for System {
    fn read(&self) -> Result<time::Unix<time::Nanos>, Error> {
        let now = SystemTime::now();
        let duration = match now.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration,
            Err(cause) => {
                return Err(Error::BeforeUnixEpoch {
                    delta: cause.duration(),
                });
            }
        };
        let nanos = u64::try_from(duration.as_nanos())
            .map_err(|_| Error::NanosecondsOverflow { duration })?;
        Ok(time::Unix::new(nanos))
    }
}

/// Stable civil epoch projected by one monotonic driver.
#[derive(Clone, Copy)]
pub struct Anchor<'d> {
    epoch: time::Unix<time::Nanos>,
    origin: clock::Moment<'d>,
}

impl<'d> Anchor<'d> {
    pub fn observe(&self, witness: &clock::Moment<'d>) -> Result<time::Unix<time::Nanos>, Error> {
        let elapsed = witness
            .try_since(self.origin)
            .ok_or_else(|| Error::MonotonicRegression {
                origin: self.origin.into_raw(),
                observed: witness.into_raw(),
            })?;
        Ok(self.epoch.try_add(elapsed)?)
    }
}

const _: () = {
    assert!(std::mem::size_of::<System>() == 0);
    assert!(
        std::mem::size_of::<Anchor<'static>>()
            == std::mem::size_of::<time::Unix<time::Nanos>>()
                + std::mem::size_of::<clock::Moment<'static>>()
    );
    assert!(!std::mem::needs_drop::<Anchor<'static>>());
};
