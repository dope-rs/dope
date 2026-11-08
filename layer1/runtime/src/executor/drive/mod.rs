use dope_core::driver::schedule::clock::Sample;

pub(crate) mod block;
pub mod clock;
mod error;
pub mod regress;
pub(crate) mod serve;
pub(crate) mod turn;

pub use error::Error;

pub(crate) fn tick<C: clock::Source>(source: C) -> Result<Sample, Error> {
    Ok(source.start())
}

pub(crate) fn timeout<C: clock::Expiring>(source: C) -> Result<Sample, Error> {
    source.tick().map_err(Error::timeout)
}

pub(crate) fn quiesce<C: clock::Expiring>(source: C) -> Result<Sample, Error> {
    source.tick().map_err(Error::quiesce)
}
