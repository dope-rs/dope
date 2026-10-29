use dope_core::driver::{life::domain, result};

/// Failure to construct an executor and its optional shutdown source.
pub enum Error<'a> {
    Driver(&'a mut result::CreateError),
    Shutdown(domain::Register),
}

impl Error<'_> {
    pub fn kind(&self) -> super::Kind<'_> {
        match self {
            Self::Driver(error) => super::Kind::Driver(error.kind()),
            Self::Shutdown(error) => super::Kind::Shutdown(error),
        }
    }
}

impl<'a> From<&'a mut result::CreateError> for Error<'a> {
    fn from(error: &'a mut result::CreateError) -> Self {
        Self::Driver(error)
    }
}

impl From<domain::Register> for Error<'_> {
    fn from(error: domain::Register) -> Self {
        Self::Shutdown(error)
    }
}
