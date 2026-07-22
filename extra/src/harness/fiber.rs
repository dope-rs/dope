use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use dope::manifold::timer::Timer;
use dope_fiber::{Either, Fiber, TimerExt as _};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed;

impl Display for Elapsed {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("test deadline elapsed")
    }
}

impl Error for Elapsed {}

/// Bounds a fiber by a timer owned by the same runtime domain.
///
/// Dropping the losing side of the race cancels its timer ticket or pending
/// operation through the ordinary [`Fiber`] drop path.
#[dope_fiber::fiber_fn('d)]
pub async fn within<'a, 'd, F, const ID: u8>(
    timer: &'a Timer<'d, ID>,
    duration: Duration,
    fiber: F,
) -> Result<F::Output, Elapsed>
where
    F: Fiber<'d> + 'a,
{
    match dope_fiber::race(fiber, timer.sleep(duration)).await {
        Either::Left(output) => Ok(output),
        Either::Right(()) => Err(Elapsed),
    }
}

/// Polls a fiber exactly once and returns that poll without driving it again.
///
/// This is useful for deterministic test barriers: a waiter can be registered
/// before the scripted peer is allowed to publish the event it waits for.
pub fn poll_once<'a, 'd, F>(fiber: &'a mut F) -> impl Fiber<'d, Output = Poll<F::Output>> + 'a
where
    F: Fiber<'d> + Unpin + 'a,
    'd: 'a,
{
    dope_fiber::poll_fn(move |cx| Poll::Ready(Fiber::poll(Pin::new(&mut *fiber), cx)))
}

/// Polls a fiber exactly once and fails the test if it completes immediately.
#[dope_fiber::fiber_fn('d)]
pub async fn expect_pending<'a, 'd, F>(fiber: &'a mut F)
where
    F: Fiber<'d> + Unpin + 'a,
    'd: 'a,
{
    let outcome = poll_once(fiber).await;
    assert!(
        outcome.is_pending(),
        "fiber completed before the test barrier"
    );
}
