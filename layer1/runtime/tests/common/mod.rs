use std::convert::Infallible;

use dope_core::driver::life::recovery::Recovery;

pub mod civil;
pub mod fork;
pub mod process;
pub mod random;
pub mod runtime;
pub mod scenario;
pub mod shutdown;
pub mod work;

pub fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause());
}
