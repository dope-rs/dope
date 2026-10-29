use std::time;

use dope_core::driver::schedule::clock;

pub(crate) struct Elapsed;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Range {
    origin: time::Instant,
    duration: time::Duration,
}

impl Range {
    pub(crate) fn try_add(
        origin: time::Instant,
        duration: time::Duration,
    ) -> Result<time::Instant, Self> {
        match origin.checked_add(duration) {
            Some(deadline) => Ok(deadline),
            None => Err(Self { origin, duration }),
        }
    }

    pub const fn origin(self) -> time::Instant {
        self.origin
    }

    pub const fn duration(self) -> time::Duration {
        self.duration
    }
}

pub(crate) trait Time: Copy {
    fn start(self) -> clock::Sample;

    fn park(self) -> time::Instant;

    fn limit(self, timeout: Option<time::Duration>) -> Option<time::Duration>;
}

pub(crate) trait Source: Time {
    fn sample(self) -> Reading;

    fn tick(self) -> clock::Sample {
        self.start()
    }
}

pub(crate) trait Expiring: Time {
    fn tick(self) -> Result<clock::Sample, Elapsed>;
}

pub(crate) struct Reading {
    tick: clock::Sample,
    wall: time::Instant,
}

#[derive(Clone, Copy)]
pub(crate) struct Wall;

impl Time for Wall {
    fn start(self) -> clock::Sample {
        self.sample().tick
    }

    fn park(self) -> time::Instant {
        self.sample().wall
    }

    fn limit(self, timeout: Option<time::Duration>) -> Option<time::Duration> {
        timeout
    }
}

impl Source for Wall {
    fn sample(self) -> Reading {
        let wall = time::Instant::now();
        Reading {
            tick: clock::Sample::new(wall),
            wall,
        }
    }
}

#[derive(Clone, Copy)]
struct Fixed<'d> {
    now: clock::Deadline<'d>,
}

impl Time for Fixed<'_> {
    fn start(self) -> clock::Sample {
        clock::Sample::new(self.now.into_raw())
    }

    fn park(self) -> time::Instant {
        self.now.into_raw()
    }

    fn limit(self, timeout: Option<time::Duration>) -> Option<time::Duration> {
        timeout.filter(time::Duration::is_zero)
    }
}

impl Source for Fixed<'_> {
    fn sample(self) -> Reading {
        Reading {
            tick: clock::Sample::new(self.now.into_raw()),
            wall: time::Instant::now(),
        }
    }
}

pub(crate) fn wall() -> Wall {
    Wall
}

pub(crate) fn at<'d>(now: clock::Deadline<'d>) -> impl Source + use<'d> {
    Fixed { now }
}

#[derive(Clone, Copy)]
pub(crate) struct Bound<S, L> {
    source: S,
    limit: L,
}

pub(crate) trait Limit: Copy {
    fn left(self, wall: time::Instant) -> time::Duration;
}

#[derive(Clone, Copy)]
pub(crate) struct Span {
    started: time::Instant,
    timeout: time::Duration,
}

impl Span {
    fn new(timeout: time::Duration) -> Self {
        Self {
            started: time::Instant::now(),
            timeout,
        }
    }
}

impl Limit for Span {
    fn left(self, wall: time::Instant) -> time::Duration {
        self.timeout
            .saturating_sub(wall.saturating_duration_since(self.started))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct End(time::Instant);

impl End {
    fn try_new(duration: time::Duration) -> Result<Self, Range> {
        let origin = time::Instant::now();
        Range::try_add(origin, duration).map(Self)
    }
}

impl Limit for End {
    fn left(self, wall: time::Instant) -> time::Duration {
        self.0.saturating_duration_since(wall)
    }
}

impl<S, L> Bound<S, L> {
    const fn new(source: S, limit: L) -> Self {
        Self { source, limit }
    }
}

impl<S> Bound<S, Span> {
    pub(crate) fn for_duration(source: S, timeout: time::Duration) -> Self {
        Self::new(source, Span::new(timeout))
    }
}

impl<S> Bound<S, End> {
    pub(crate) fn until(source: S, timeout: time::Duration) -> Result<Self, Range> {
        let end = End::try_new(timeout)?;
        Ok(Self::new(source, end))
    }
}

impl<S, L> Time for Bound<S, L>
where
    S: Source,
    L: Limit,
{
    fn start(self) -> clock::Sample {
        self.source.start()
    }

    fn park(self) -> time::Instant {
        self.source.park()
    }

    fn limit(self, timeout: Option<time::Duration>) -> Option<time::Duration> {
        let left = self.limit.left(time::Instant::now());
        Some(
            self.source
                .limit(timeout)
                .map_or(left, |wait| wait.min(left)),
        )
    }
}

impl<S, L> Expiring for Bound<S, L>
where
    S: Source,
    L: Limit,
{
    fn tick(self) -> Result<clock::Sample, Elapsed> {
        let reading = self.source.sample();
        if self.limit.left(reading.wall).is_zero() {
            Err(Elapsed)
        } else {
            Ok(reading.tick)
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Wall>() == 0);
    assert!(std::mem::size_of::<Fixed<'_>>() == std::mem::size_of::<clock::Deadline<'_>>());
    assert!(std::mem::size_of::<Range>() == std::mem::size_of::<(time::Instant, time::Duration)>());
    assert!(std::mem::size_of::<Bound<(), End>>() == std::mem::size_of::<time::Instant>());
    assert!(
        std::mem::size_of::<Bound<(), Span>>()
            == std::mem::size_of::<(time::Instant, time::Duration)>()
    );
};
