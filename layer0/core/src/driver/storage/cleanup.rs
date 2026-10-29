use std::{convert, mem};

use dope_sys::memory;

use crate::{
    Resources,
    driver::{life::native::create, storage},
};

/// Construction failure that still owns backend resources requiring cleanup.
pub struct Cleanup(State<Resources, create::Failure>);

impl Cleanup {
    pub(in crate::driver) fn new<S: Into<Resources>>(state: S) -> Self {
        Self(State::new(state))
    }

    /// Retries in place; the native owner remains resident until cleanup succeeds.
    pub fn try_close(
        &mut self,
        report: impl FnMut(storage::Failure),
    ) -> Result<storage::Failure, storage::Failure> {
        self.0
            .try_close(report, storage::Failure::from, storage::Failure::from)
    }

    pub fn primary(&self) -> storage::Failure {
        self.0.primary(storage::Failure::from)
    }

    /// No pending failure remains after terminal cleanup.
    pub fn cleanup(&self) -> Option<storage::Failure> {
        self.0.failure(storage::Failure::from)
    }
}

/// The native owner and its terminal primary occupy mutually exclusive retry phases.
pub(super) enum State<R, P> {
    Owner(R),
    Primary(P),
}

impl<R, P> State<R, P> {
    pub(super) fn new<S: Into<R>>(state: S) -> Self {
        Self::Owner(state.into())
    }
}

impl<R: memory::Cleanup<P>, P: Copy> State<R, P> {
    pub(super) fn try_close<T, E>(
        &mut self,
        mut report: impl FnMut(E),
        primary: impl FnOnce(P) -> T,
        failure: impl Fn(R::Error) -> E,
    ) -> Result<T, E> {
        let retained = self.primary(convert::identity);
        match mem::replace(self, Self::Primary(retained)) {
            Self::Primary(value) => Ok(primary(value)),
            Self::Owner(owner) => match owner.try_close(|error| report(failure(error))) {
                Ok(value) => {
                    *self = Self::Primary(value);
                    Ok(primary(value))
                }
                Err(owner) => {
                    let error = owner.failure();
                    *self = Self::Owner(owner);
                    Err(failure(error))
                }
            },
        }
    }

    pub(super) fn primary<T>(&self, map: impl FnOnce(P) -> T) -> T {
        map(match self {
            Self::Owner(owner) => owner.primary(),
            Self::Primary(primary) => *primary,
        })
    }

    pub(super) fn failure<E>(&self, map: impl FnOnce(R::Error) -> E) -> Option<E> {
        match self {
            Self::Owner(owner) => Some(map(owner.failure())),
            Self::Primary(_) => None,
        }
    }
}
