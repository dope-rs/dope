use std::pin::Pin;
use std::time::{Duration, Instant};

use dope_core::driver::OutboundReservation;
use dope_core::driver::token::Token;
use dope_core::io::Event;
use dope_core::io::fd::Fd;
use o3::cell::RegionToken;

use crate::DriverContext;

/// Post-dispatch access to driver-owned resource retirement.
///
/// Cooperative shutdown constructs it after draining. Scope-drop paths
/// construct it only after ending further callbacks into the dispatcher.
pub struct FinishContext<'a, 'd> {
    driver: DriverContext<'a, 'd>,
}

impl<'a, 'd> FinishContext<'a, 'd> {
    pub(crate) fn new(driver: DriverContext<'a, 'd>) -> Self {
        Self { driver }
    }

    pub fn retire_outbound(&mut self, reservation: OutboundReservation<'d>) {
        self.driver.retire_outbound(reservation);
    }

    pub fn retire_fixed_fd(&mut self, fd: &mut Fd<'d>) {
        self.driver.retire_fixed_fd(fd);
    }
}

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

/// Application-side hooks driven by a [`Session`](super::executor::Session).
///
/// The runtime serializes every callback on its worker thread. `shutdown` is
/// invoked at most once for an app scope, including interruption and unwinding
/// paths managed by [`Session::with_app`](super::executor::Session::with_app).
pub trait Dispatcher<'d>: Sized {
    const SHUTDOWN_DRAIN: Duration = Duration::from_secs(2);

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>);

    fn activate(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>);

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle;

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let _ = driver;
    }

    fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        let _ = context;
    }
}
