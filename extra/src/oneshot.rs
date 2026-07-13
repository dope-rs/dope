use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};

use dope::Driver;
use dope::runtime::park;
use dope::runtime::park::Parker;
use dope::runtime::token::{Epoch, LocalIdx, Token};
use pin_project::pin_project;

#[pin_project]
pub struct OneShot<F: Future> {
    #[pin]
    fut: Option<F>,
    output: Option<F::Output>,
    slot: park::Slot,
    _pin: PhantomPinned,
}

impl<F: Future> OneShot<F> {
    pub fn new(fut: F, route: u8, driver: &Driver) -> Self {
        let target = Token::new(route, LocalIdx::new(0), Epoch::INITIAL);
        Self {
            fut: Some(fut),
            output: None,
            slot: Parker::make_slot(driver, target),
            _pin: PhantomPinned,
        }
    }

    pub fn is_done(&self) -> bool {
        self.fut.is_none()
    }

    pub fn take_output(self: Pin<&mut Self>) -> Option<F::Output> {
        let this = self.project();
        this.output.take()
    }

    pub fn pre_park(self: Pin<&mut Self>, _driver: &Driver) {
        let mut this = self.project();
        let Some(fut) = this.fut.as_mut().as_pin_mut() else {
            return;
        };
        let waker = this.slot.make_waker();
        let mut cx = Context::from_waker(&waker);
        if let Poll::Ready(v) = fut.poll(&mut cx) {
            *this.output = Some(v);
            this.fut.set(None);
        }
    }
}
