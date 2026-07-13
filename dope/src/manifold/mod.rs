mod buf;

pub mod connector;
pub mod datagram;
pub mod env;
pub mod file;
pub mod listener;

pub mod route;
pub mod timer;

use std::pin::Pin;

use crate::runtime::dispatcher::Idle;
use crate::{Driver, backend};

pub enum Outcome {
    Ok,
    Overrun,
    CloseAfter,
}

pub trait Manifold<'d>: Sized {
    const ID: u8 = 0;

    fn dispatch(self: Pin<&mut Self>, ev: backend::Event, driver: &'d Driver) {
        let _ = (self, ev, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &'d Driver);

    fn idle(self: Pin<&Self>) -> Idle {
        let _ = self;
        Idle::Park(None)
    }

    fn on_wake(self: Pin<&mut Self>, target: route::TypedToken<Self>, driver: &'d Driver) {
        let _ = (target, driver);
    }

    fn on_shutdown(self: Pin<&mut Self>, driver: &'d Driver) {
        let _ = (self, driver);
    }
}
