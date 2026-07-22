use core::pin::Pin;
use std::io;

use dope::driver::ready::ReadySlot;
use dope::driver::token::{Epoch, SlotIndex, Token, kind};
use dope::runtime::__private::RootTask;
use dope::{DriverContext, DriverRef};
use pin_project::pin_project;

use crate::{Context, Fiber};

#[pin_project]
pub struct OneShot<'d, F>
where
    F: Fiber<'d>,
{
    #[pin]
    fiber: Option<F>,
    output: Option<F::Output>,
    #[pin]
    slot: ReadySlot<'d>,
    target: Token,
}

impl<'d, F> OneShot<'d, F>
where
    F: Fiber<'d>,
{
    pub fn new(fiber: F, route: u8, driver: DriverRef<'d>) -> io::Result<Self> {
        let target = Token::new(route, SlotIndex::new(0), Epoch::INITIAL).with_kind(kind::ONE_SHOT);
        Ok(Self {
            fiber: Some(fiber),
            output: None,
            slot: driver.make_ready_slot(target)?,
            target,
        })
    }

    pub fn target(&self) -> Token {
        self.target
    }

    pub fn is_done(&self) -> bool {
        self.fiber.is_none()
    }

    pub fn take_output(self: Pin<&mut Self>) -> Option<F::Output> {
        self.project().output.take()
    }

    pub fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let this = unsafe { self.get_unchecked_mut() };
        let slot = unsafe { Pin::new_unchecked(&this.slot) };
        let mut context = core::pin::pin!(Context::from_ready(
            driver.driver_ref(),
            slot.key(),
            driver.reborrow(),
        ));
        let Some(fiber) = this.fiber.as_mut() else {
            return;
        };
        if let core::task::Poll::Ready(output) =
            Fiber::poll(unsafe { Pin::new_unchecked(fiber) }, context.as_mut())
        {
            this.output = Some(output);
            this.fiber = None;
        }
    }
}

impl<'d, F> RootTask<'d, F::Output> for OneShot<'d, F>
where
    F: Fiber<'d>,
{
    fn target(&self) -> Token {
        self.target()
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.pre_park(driver);
    }

    fn take_output(self: Pin<&mut Self>) -> Option<F::Output> {
        self.take_output()
    }
}
