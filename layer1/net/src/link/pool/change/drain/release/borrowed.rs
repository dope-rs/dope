use dope_core::driver::retained;
use o3::base::cell::region;

use crate::link::pool::change::drain::release;

/// Returns a payload under the driver's original region authority.
#[repr(transparent)]
pub struct Borrowed<F> {
    release: F,
}

impl<F> Borrowed<F> {
    pub const fn new<'d, X, B>(release: F) -> Self
    where
        F: FnMut(&mut X, B, &mut region::Token<region::Identity<'d, 1>>),
    {
        Self { release }
    }
}

impl<'d, X, B, F: FnMut(&mut X, B, &mut region::Token<region::Identity<'d, 1>>)>
    release::Policy<'d, X, B> for Borrowed<F>
{
    fn release(&mut self, context: &mut X, payload: B, driver: &mut retained::Context<'_, '_, 'd>) {
        (self.release)(context, payload, driver.state_token());
    }
}
