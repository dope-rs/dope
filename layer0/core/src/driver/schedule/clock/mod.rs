//! Driver-branded monotonic time sampled once per runtime turn.

use std::{borrow, cell, cmp, marker, mem, ops, time};

use o3::base::marker::ThreadBound;

use crate::driver;

mod range;
pub mod timer;
#[doc(hidden)]
pub mod watchdog;

pub use range::Range;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
struct Point<'d> {
    at: time::Instant,
    driver: (driver::Invariant<'d>, ThreadBound),
}
impl PartialOrd for Point<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Point<'_> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at.cmp(&other.at)
    }
}
/// Read-only access to the monotonic sample installed for one driver turn.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Clock<'turn, 'd> {
    source: &'turn cell::Cell<time::Instant>,
    driver: (driver::Invariant<'d>, ThreadBound),
}
/// Generative monotonic domain for code running outside a runtime driver.
///
#[derive(Clone, Copy)]
pub struct Scope<'d> {
    proof: (driver::Invariant<'d>, ThreadBound),
}
/// Owned monotonic origin that accepts no foreign time-domain input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Origin(time::Instant);

/// A persistent monotonic observation belonging to exactly one driver scope.
///
/// Driver brands are invariant, so observations from independently generated
/// scopes cannot be compared or used to derive one another's deadlines:
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Moment<'d>(Point<'d>);
/// A persistent absolute monotonic deadline belonging to one driver scope.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Deadline<'d>(Point<'d>);
/// A raw-time owner attached to exactly one driver scope.
///
#[repr(transparent)]
pub struct Bound<'d, T> {
    inner: T,
    driver: (driver::Invariant<'d>, ThreadBound),
}
/// A lexical view combining one attached owner with one branded observation.
#[repr(C)]
pub struct At<'d, A> {
    access: A,
    now: Moment<'d>,
}

/// One raw runtime-boundary sample waiting to be installed into a turn.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Sample(time::Instant);

impl Sample {
    /// Wraps the raw monotonic sample for installation into a runtime turn.
    pub const fn new(now: time::Instant) -> Self {
        Self(now)
    }

    pub(in crate::driver) const fn into_raw(self) -> time::Instant {
        self.0
    }
}

impl Scope<'_> {
    /// Runs with a fresh time domain whose observations cannot escape.
    pub fn run<R>(run: impl for<'d> FnOnce(Scope<'d>) -> R) -> R {
        run(Scope::new())
    }
}

impl<'d> Scope<'d> {
    const fn new() -> Self {
        Self {
            proof: (driver::Invariant(marker::PhantomData), ThreadBound::NEW),
        }
    }

    pub fn now(self) -> Moment<'d> {
        Moment::new(time::Instant::now(), self.proof)
    }
}

impl Origin {
    pub fn elapsed(self) -> time::Duration {
        self.0.elapsed()
    }
}

impl<'d> Point<'d> {
    const fn new(at: time::Instant, driver: (driver::Invariant<'d>, ThreadBound)) -> Self {
        Self { at, driver }
    }

    const fn at(self, at: time::Instant) -> Self {
        Self::new(at, self.driver)
    }

    const fn get(self) -> time::Instant {
        self.at
    }

    fn try_add<D: Copy + borrow::Borrow<time::Duration>>(
        self,
        duration: D,
    ) -> Result<Self, Range<D>> {
        match self.at.checked_add(*duration.borrow()) {
            Some(at) => Ok(self.at(at)),
            None => Err(Range::new(Origin(self.at), duration)),
        }
    }

    fn clamp_add<D: Copy + borrow::Borrow<time::Duration>>(self, duration: D) -> Self {
        match self.try_add(duration) {
            Ok(point) => point,
            Err(range) => self.at(range.furthest()),
        }
    }
}

impl<'turn, 'd> Clock<'turn, 'd> {
    pub(in crate::driver) const fn new(
        source: &'turn cell::Cell<time::Instant>,
        driver: driver::Invariant<'d>,
    ) -> Self {
        Self {
            source,
            driver: (driver, ThreadBound::NEW),
        }
    }

    /// Returns the one sample installed at the beginning of this turn.
    pub fn now(self) -> Moment<'d> {
        Moment::new(self.source.get(), self.driver)
    }

    /// Attaches a raw-time owner to this clock's driver.
    pub fn attach<T>(self, build: impl FnOnce(time::Instant) -> T) -> Bound<'d, T> {
        Bound::new(build(self.source.get()), self.driver)
    }

    /// Attaches a fallibly constructed raw-time owner to this driver.
    pub fn try_attach<T, E>(
        self,
        build: impl FnOnce(time::Instant) -> Result<T, E>,
    ) -> Result<Bound<'d, T>, E> {
        build(self.source.get()).map(|inner| Bound::new(inner, self.driver))
    }

    /// Adopts an external absolute instant as this driver's deadline.
    pub fn deadline_at(self, at: time::Instant) -> Deadline<'d> {
        Deadline(Point::new(at, self.driver))
    }

    /// Binds lexical access to this turn's canonical sample.
    pub fn at<A>(self, access: A) -> At<'d, A> {
        At {
            access,
            now: self.now(),
        }
    }
}

impl<'d, T> Bound<'d, T> {
    const fn new(inner: T, driver: (driver::Invariant<'d>, ThreadBound)) -> Self {
        Self { inner, driver }
    }

    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Bound<'d, U> {
        Bound::new(map(self.inner), self.driver)
    }
}

impl<T> ops::Deref for Bound<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> ops::DerefMut for Bound<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'d, A> At<'d, A> {
    pub fn apply<R>(self, apply: impl FnOnce(A, time::Instant) -> R) -> R {
        apply(self.access, self.now.0.get())
    }

    pub fn deadline(
        self,
        project: impl FnOnce(A, time::Instant) -> Option<time::Instant>,
    ) -> Option<Deadline<'d>> {
        project(self.access, self.now.0.get()).map(|at| Deadline(self.now.0.at(at)))
    }
}

impl<'d, T: ?Sized> At<'d, &mut T> {
    /// Lends the owner with its original driver observation.
    ///
    ///
    pub fn apply_mut<R>(&mut self, apply: impl FnOnce(&mut T, Moment<'d>) -> R) -> R {
        apply(&mut *self.access, self.now)
    }
}

impl<'d> Moment<'d> {
    const fn new(at: time::Instant, driver: (driver::Invariant<'d>, ThreadBound)) -> Self {
        Self(Point::new(at, driver))
    }

    /// Retains this observation without its driver brand.
    pub const fn retain(self) -> Origin {
        Origin(self.0.get())
    }

    pub fn try_add<D: Copy + borrow::Borrow<time::Duration>>(
        self,
        duration: D,
    ) -> Result<Deadline<'d>, Range<D>> {
        Ok(Deadline(self.0.try_add(duration)?))
    }

    /// Adds a duration, clamping only at the platform's furthest instant.
    pub fn clamp_add<D: Copy + borrow::Borrow<time::Duration>>(self, duration: D) -> Deadline<'d> {
        Deadline(self.0.clamp_add(duration))
    }

    /// Binds lexical access to this persistent observation.
    pub fn at<A>(self, access: A) -> At<'d, A> {
        At { access, now: self }
    }

    pub fn moment_at(self, at: time::Instant) -> Self {
        Self(self.0.at(at))
    }

    pub fn deadline_at(self, at: time::Instant) -> Deadline<'d> {
        Deadline(self.0.at(at))
    }

    pub fn since(self, earlier: Self) -> time::Duration {
        self.0.get().duration_since(earlier.0.get())
    }

    pub fn clamp_since(self, earlier: Self) -> time::Duration {
        self.0.get().saturating_duration_since(earlier.0.get())
    }

    pub fn try_since(self, earlier: Self) -> Option<time::Duration> {
        self.0.get().checked_duration_since(earlier.0.get())
    }

    pub fn past_due(self, earlier: Deadline<'d>) -> time::Duration {
        self.0.get().duration_since(earlier.0.get())
    }

    /// Exposes the underlying monotonic sample at a runtime boundary.
    pub const fn into_raw(self) -> time::Instant {
        self.0.get()
    }
}

impl<'d> Deadline<'d> {
    pub fn try_add<D: Copy + borrow::Borrow<time::Duration>>(
        self,
        duration: D,
    ) -> Result<Self, Range<D>> {
        Ok(Self(self.0.try_add(duration)?))
    }

    /// Adds a duration, clamping only at the platform's furthest instant.
    pub fn clamp_add<D: Copy + borrow::Borrow<time::Duration>>(self, duration: D) -> Self {
        Self(self.0.clamp_add(duration))
    }

    pub fn checked_sub(self, duration: time::Duration) -> Option<Self> {
        self.0
            .get()
            .checked_sub(duration)
            .map(|at| Self(self.0.at(at)))
    }

    pub fn is_due(self, now: Moment<'d>) -> bool {
        self.0.get() <= now.0.get()
    }

    pub fn try_left(self, now: Moment<'d>) -> Option<time::Duration> {
        self.0.get().checked_duration_since(now.0.get())
    }

    /// Exposes the underlying monotonic deadline at a runtime boundary.
    pub const fn into_raw(self) -> time::Instant {
        self.0.get()
    }
}

impl From<Moment<'_>> for time::Instant {
    fn from(moment: Moment<'_>) -> Self {
        moment.0.at
    }
}

impl<'d> From<Moment<'d>> for Deadline<'d> {
    fn from(moment: Moment<'d>) -> Self {
        Self(moment.0)
    }
}

impl<'d> PartialEq<Moment<'d>> for Deadline<'d> {
    fn eq(&self, other: &Moment<'d>) -> bool {
        self.0.get() == other.0.get()
    }
}

impl<'d> PartialOrd<Moment<'d>> for Deadline<'d> {
    fn partial_cmp(&self, other: &Moment<'d>) -> Option<cmp::Ordering> {
        self.0.get().partial_cmp(&other.0.get())
    }
}

impl<'d> PartialEq<Deadline<'d>> for Moment<'d> {
    fn eq(&self, other: &Deadline<'d>) -> bool {
        self.0.get() == other.0.get()
    }
}

impl<'d> PartialOrd<Deadline<'d>> for Moment<'d> {
    fn partial_cmp(&self, other: &Deadline<'d>) -> Option<cmp::Ordering> {
        self.0.get().partial_cmp(&other.0.get())
    }
}

const _: () = {
    assert!(mem::size_of::<Scope<'static>>() == 0);
    assert!(mem::size_of::<Origin>() == mem::size_of::<time::Instant>());
    assert!(mem::align_of::<Origin>() == mem::align_of::<time::Instant>());
    assert!(!mem::needs_drop::<Origin>());
    assert!(mem::size_of::<Clock<'static, 'static>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Clock<'static, 'static>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Moment<'static>>() == mem::size_of::<time::Instant>());
    assert!(mem::align_of::<Moment<'static>>() == mem::align_of::<time::Instant>());
    assert!(mem::size_of::<Deadline<'static>>() == mem::size_of::<time::Instant>());
    assert!(mem::align_of::<Deadline<'static>>() == mem::align_of::<time::Instant>());
    assert!(!mem::needs_drop::<Moment<'static>>());
    assert!(!mem::needs_drop::<Deadline<'static>>());
    assert!(
        mem::size_of::<At<'static, &'static mut ()>>()
            == mem::size_of::<&mut ()>() + mem::size_of::<Moment<'static>>()
    );
    assert!(mem::align_of::<At<'static, &'static mut ()>>() == mem::align_of::<time::Instant>());
    assert!(!mem::needs_drop::<At<'static, &'static mut ()>>());
    assert!(mem::size_of::<Bound<'static, time::Instant>>() == mem::size_of::<time::Instant>());
    assert!(mem::align_of::<Bound<'static, time::Instant>>() == mem::align_of::<time::Instant>());
    assert!(mem::size_of::<Sample>() == mem::size_of::<time::Instant>());
};
