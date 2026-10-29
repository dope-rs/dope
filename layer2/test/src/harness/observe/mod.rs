use std::{cell, task::Poll};

use dope_core::driver::{route, route::kind, storage};
use dope_runtime::executor::{drive, root, session};
use o3::depot::array;

use crate::harness::scenario::TIMEOUT;

mod raw;

/// Unbounded observations retained by an exclusively owned vector.
pub trait VecCellExt<T> {
    fn push(&self, value: T);
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> VecCellExt<T> for cell::Cell<Vec<T>> {
    fn push(&self, value: T) {
        let mut values = self.take();
        values.push(value);
        self.set(values);
    }

    fn len(&self) -> usize {
        let values = self.take();
        let len = values.len();
        self.set(values);
        len
    }
}

/// Fixed-capacity, stack-backed event history.
#[must_use = "inspect the recorded observations and any rejected event"]
pub struct Trace<T, const N: usize> {
    values: cell::RefCell<array::Vec<T, N>>,
    failure: cell::Cell<Option<(T, Error)>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Full,
    Borrowed,
}

impl<T, const N: usize> Default for Trace<T, N> {
    fn default() -> Self {
        Self {
            values: cell::RefCell::default(),
            failure: cell::Cell::new(None),
        }
    }
}

impl<T, const N: usize> Trace<T, N> {
    /// Creates an empty fixed-capacity trace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an observation or returns it unchanged when full.
    pub fn push(&self, value: T) -> Result<(), (T, Error)> {
        match self.values.try_borrow_mut() {
            Ok(mut values) => values.push(value).map_err(|value| (value, Error::Full)),
            Err(_) => Err((value, Error::Borrowed)),
        }
    }

    /// Retains the first rejection until the caller inspects the observations.
    pub fn record(&self, value: T) {
        if let Err(rejected) = self.push(value)
            && let Some(first) = self.failure.replace(Some(rejected))
        {
            self.failure.set(Some(first));
        }
    }

    /// Borrows the observations in insertion order.
    pub fn values(&self) -> Result<cell::Ref<'_, [T]>, Error> {
        let failure = self.failure.take();
        let error = failure.as_ref().map(|(_, error)| *error);
        self.failure.set(failure);
        if let Some(error) = error {
            return Err(error);
        }
        let values = self.values.try_borrow().map_err(|_| Error::Borrowed)?;
        Ok(cell::Ref::map(values, |values| values.as_slice()))
    }
}

impl<'d, const N: usize> Trace<route::Erased<'d, kind::Any>, N> {
    /// Collects route-zero activations through application retirement.
    pub fn try_drain<'report, 'scope, 'life: 'scope, 'p: 'life, S: storage::Plan + 'd>(
        report: &'report mut session::Completion,
        scope: &mut session::Scope<'scope, 'life, 'p, 'd, S>,
    ) -> Result<Self, Drain<'report>>
    where
        'd: 'life,
    {
        let values = Self::new();
        scope
            .with_quiesce(report, &values, TIMEOUT, |mut app| app.drive(Ready))
            .map_err(Drain::Application)?
            .map_err(Drain::Root)?;
        Ok(values)
    }

    /// Checks the exact activation identities in observation order.
    pub fn matches(
        &self,
        expected: &[route::Operation<'d, route::KeyTag<0>>],
    ) -> Result<bool, Error> {
        let values = self.values()?;
        Ok(values.len() == expected.len()
            && values
                .iter()
                .zip(expected)
                .all(|(&actual, expected)| expected.matches(actual)))
    }
}

#[derive(Debug)]
pub enum Drain<'report> {
    Application(session::Error<'report, Result<(), drive::Error>>),
    Root(drive::Error),
}

struct Ready;

impl<'d> root::Root<'d> for Ready {
    type Output = ();

    fn poll(_: root::Context<'_, 'd, Self>) -> Poll<()> {
        Poll::Ready(())
    }
}
