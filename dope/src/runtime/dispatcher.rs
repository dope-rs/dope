use std::pin::Pin;
use std::time::Instant;

use crate::{Driver, backend};

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

pub trait Dispatcher<'d> {
    const SHUTDOWN_DRAIN: std::time::Duration = std::time::Duration::from_secs(2);

    fn dispatch(self: Pin<&mut Self>, ev: backend::Event, driver: &'d Driver);

    fn on_wake(self: Pin<&mut Self>, target: backend::token::Token, driver: &'d Driver);

    fn pre_park(self: Pin<&mut Self>, driver: &'d Driver);

    fn idle(self: Pin<&Self>) -> Idle;

    fn on_shutdown(self: Pin<&mut Self>, driver: &'d Driver) {
        let _ = (self, driver);
    }
}
