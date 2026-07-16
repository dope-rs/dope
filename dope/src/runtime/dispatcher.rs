use std::pin::Pin;
use std::time::{Duration, Instant};

use crate::DriverContext;
use dope_core::driver::token::Token;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idle {
    Busy,
    Park(Option<Instant>),
}

impl Idle {
    pub fn reduce(self, other: Self) -> Self {
        match (self, other) {
            (Idle::Busy, _) | (_, Idle::Busy) => Idle::Busy,
            (Idle::Park(a), Idle::Park(b)) => Idle::Park(match (a, b) {
                (Some(x), Some(y)) => Some(x.min(y)),
                (a, b) => a.or(b),
            }),
        }
    }
}

/// Application-side hooks driven by a [`Session`](super::Session).
///
/// The runtime serializes every callback on its worker thread. `shutdown` is
/// invoked at most once for an app scope, including interruption and unwinding
/// paths managed by [`Session::with_app`](super::Session::with_app).
pub trait Dispatcher<'d>: Sized {
    const SHUTDOWN_DRAIN: Duration = Duration::from_secs(2);

    fn dispatch(self: Pin<&mut Self>, ev: dope_core::io::Event, driver: &mut DriverContext<'_, 'd>);

    fn activate(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>);

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn idle(self: Pin<&Self>) -> Idle;

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let _ = driver;
    }
}
