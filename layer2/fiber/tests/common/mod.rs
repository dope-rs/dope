use dope_core::driver::life::recovery;
use std::convert::Infallible;

pub mod file;
pub mod network;
pub mod schedule;
pub mod storage;

pub mod runtime;

pub fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
