use dope_core::driver::life::recovery;
use std::convert::Infallible;

pub mod contract;
pub mod manifold;
pub mod scenario;

mod buffer;
pub mod observed;
pub mod requests;
pub mod resident;
mod sequence;

pub use buffer::Buffer;
pub use sequence::Sequence;

pub fn recover(failure: recovery::Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
