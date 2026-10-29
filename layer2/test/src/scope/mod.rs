//! Driver scopes with deterministic native resource retirement.

use std::convert::Infallible;

use dope_core::driver::{life::domain, result};
use dope_runtime::{executor::create, topology::error::Cause};

/// Original startup or teardown failure, borrowed while its native owner remains retained.
pub enum Error<'owner, R> {
    Create(&'owner mut result::CreateError),
    Close(domain::Error<'owner, Infallible, R>),
}

impl<R> Error<'_, R> {
    /// Borrows diagnostics without consuming retained output or native cleanup authority.
    pub fn cause(&self) -> Cause<'_, Infallible> {
        match self {
            Self::Create(error) => Cause::Runtime(create::Kind::Driver(error.kind())),
            Self::Close(error) => match error.cause() {
                domain::Cause::Storage(error) => Cause::Storage(error),
                domain::Cause::Close { storage, failure } => Cause::Close { storage, failure },
            },
        }
    }
}

mod driver;
pub use driver::Driver;
mod scoped;
pub use scoped::Scoped;
