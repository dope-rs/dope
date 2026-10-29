use std::convert::Infallible;

use dope_core::driver::life::{domain, domain::Rejected};

use crate::executor::{create, drive, session};

/// Startup and teardown failures without formatting retained operation or output owners.
#[derive(Debug)]
pub enum Cause<'a, S> {
    Runtime(create::Kind<'a>),
    Storage(&'a S),
    Close {
        storage: Option<&'a S>,
        failure: domain::Unmap,
    },
}

impl<'a, S> Cause<'a, S> {
    pub(super) fn closing<O, T, Q>(state: &'a domain::Closing<'_, Rejected<S, O>, T, Q>) -> Self {
        Self::Close {
            storage: state.result().as_ref().err().map(|error| &error.cause),
            failure: state.failure(),
        }
    }
}

/// Exact application failures without formatting retained successful output.
#[derive(Debug)]
pub enum Failure<'a, I> {
    Run {
        cause: session::Cause<'a>,
        output: Option<&'a drive::Error>,
    },
    Install(&'a I),
}

/// Exact execution and teardown failures while their original owners remain retained.
#[derive(Debug)]
pub enum Run<'a, S, I> {
    Runtime(create::Kind<'a>),
    Storage(&'a S),
    Failure(Failure<'a, I>),
    Close {
        result: Result<Result<(), Failure<'a, I>>, &'a S>,
        failure: domain::Unmap,
    },
}

/// Exact startup or teardown causes borrowed without requiring retained owners to format.
#[derive(Debug)]
pub enum Kind<'a, S, T, F = Infallible> {
    Runtime(create::Kind<'a>),
    Storage(&'a S),
    Run(&'a F),
    Close {
        result: Result<T, &'a S>,
        failure: domain::Unmap,
    },
}

impl<'a, S, O, T, Q> From<&'a domain::Error<'_, Rejected<S, O>, T, Q>> for Kind<'a, S, &'a T> {
    fn from(error: &'a domain::Error<'_, Rejected<S, O>, T, Q>) -> Self {
        match error {
            domain::Error::Storage(error) => Self::Storage(&error.cause),
            domain::Error::Close(state) => Self::closing(state),
        }
    }
}

impl<'a, S, T, F> Kind<'a, S, &'a T, F> {
    pub(super) fn closing<O, Q>(state: &'a domain::Closing<'_, Rejected<S, O>, T, Q>) -> Self {
        Self::Close {
            result: state.result().as_ref().map_err(|error| &error.cause),
            failure: state.failure(),
        }
    }
}

impl<'a, S, T, F> Kind<'a, S, T, F> {
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Kind<'a, S, U, F> {
        match self {
            Self::Runtime(error) => Kind::Runtime(error),
            Self::Storage(error) => Kind::Storage(error),
            Self::Run(error) => Kind::Run(error),
            Self::Close { result, failure } => Kind::Close {
                result: result.map(map),
                failure,
            },
        }
    }
}
