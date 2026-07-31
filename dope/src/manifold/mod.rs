pub mod connector;
pub mod datagram;
pub mod env;
pub mod file;
pub mod listener;
pub mod timer;
pub mod typed;

use typed::TypedToken;

use std::pin::Pin;

use crate::DriverContext;
use crate::runtime::dispatcher::{FinishContext, Idle};
use dope_core::io::Event;

pub enum Outcome {
    Ok,
    Overrun,
    CloseAfter,
}

pub trait Manifold<'d>: Sized {
    const ID: u8 = 0;

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let _ = (ev, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>);

    fn idle(self: Pin<&Self>) -> Idle {
        Idle::Park(None)
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _ = (target, driver);
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let _ = driver;
    }

    fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        let _ = context;
    }
}
