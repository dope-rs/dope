use std::future::Future;
use std::io;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;

use dope::Driver;
use dope::fiber::Holding;
use dope::manifold::timer::Timer;
use dope::runtime::profile::{Production, Profile};
use dope::runtime::token::ROUTE_TASKS;
use pin_project::pin_project;

mod block_on;
mod oneshot;
mod slab;
pub mod testing;
mod trigger;

pub use block_on::block_on;
pub use oneshot::OneShot;
use slab::Slab;
pub use trigger::{SignalTrigger, Trigger};

#[pin_project]
pub struct Detached<P: Profile = Production> {
    exec: Box<Slab>,
    #[pin]
    timer: Timer,
    marker: std::marker::PhantomData<P>,
    _pin: PhantomPinned,
}

impl<P: Profile> Detached<P> {
    pub fn new(driver: &Driver) -> Self {
        let exec = Box::new(Slab::with_capacity(P::TASK_SLAB_CAPACITY, driver));
        Self {
            exec,
            timer: Timer::new(),
            marker: std::marker::PhantomData,
            _pin: PhantomPinned,
        }
    }

    pub fn detacher_handle(&mut self) -> Handle {
        Handle::new(NonNull::from(&mut *self.exec))
    }

    pub fn timer_handle<'d>(self: Pin<&'d mut Self>) -> Holding<'d, Timer> {
        let this = self.project();
        Holding::of(this.timer)
    }
}

impl<'d, P: Profile> dope::manifold::Manifold<'d> for Detached<P> {
    const ID: u8 = ROUTE_TASKS;

    fn pre_park(self: Pin<&mut Self>, driver: &'d Driver) {
        let this = self.project();
        this.timer.pre_park(driver);
    }

    fn on_wake(
        self: Pin<&mut Self>,
        target: dope::manifold::route::TypedToken<Self>,
        _driver: &'d Driver,
    ) {
        let this = self.project();
        this.exec.wake_slot(target.slot());
    }

    fn idle(self: Pin<&Self>) -> dope::Idle {
        self.project_ref().timer.idle()
    }
}

#[derive(Clone, Copy)]
pub struct Handle {
    slab: NonNull<Slab>,
}

impl Handle {
    fn new(slab: NonNull<Slab>) -> Self {
        Self { slab }
    }

    pub fn detach<F>(self, fut: F) -> Result<(), io::Error>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        // SAFETY: `slab` is a NonNull into a pinned `Detached`'s `exec` field, valid for the dispatcher's lifetime; thread-per-core.
        let slab = unsafe { self.slab.as_ptr().as_mut().unwrap_unchecked() };
        let drop_output = async move {
            let _ = fut.await;
        };
        if slab.try_spawn(drop_output).is_some() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Handle::detach: task slab full",
            ))
        }
    }
}
