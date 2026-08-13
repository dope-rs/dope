use std::pin;

use dope_core::driver;

pub(crate) fn scope<R>(
    driver: pin::Pin<&mut driver::Driver>,
    f: impl for<'d> FnOnce(driver::lifecycle::Scope<'d>) -> R,
) -> R {
    // SAFETY: the generative scope consumes every safe domain borrow before return.
    let owner = unsafe { driver::lifecycle::quiesce::raw::Owner::new() };
    driver.scope(driver::lifecycle::quiesce::Lease::new(owner), f)
}
