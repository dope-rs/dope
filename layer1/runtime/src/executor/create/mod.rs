use dope_core::driver::{life::domain, result};

mod error;

pub use error::Error;

#[derive(Debug)]
pub enum Kind<'a> {
    Driver(result::CreateKind<'a>),
    Shutdown(&'a domain::Register),
}
