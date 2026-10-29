use crate::ring::{layout::Layout, map::Map};

/// Setup rejection or retained ownership of incomplete mapping cleanup.
#[derive(Debug)]
pub enum Error<S, M, C> {
    Setup(S),
    Layout(Layout),
    Map(Map<M>),
    Cleanup(C),
}

impl<S, M, C> From<super::Cause<S, M>> for Error<S, M, C> {
    fn from(cause: super::Cause<S, M>) -> Self {
        match cause {
            super::Cause::Setup(cause) => Self::Setup(cause),
            super::Cause::Layout(cause) => Self::Layout(cause),
            super::Cause::Map(cause) => Self::Map(cause),
        }
    }
}
