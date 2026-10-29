use metadata::pool;
use o3::{base::cell::region, mem::Rejected};

use crate::link::egress::{
    self, data, metadata,
    reserve::{demand, guard::Guard},
};

/// One source node that owns its exact immutable payload before state changes.
#[must_use = "a source reservation must be committed or rolled back"]
pub struct Reserve<'queue, 'token, 'p, 'd, T, D: demand::Mode = demand::Fallible> {
    parts: Guard<'token, 'd, pool::Prepared<'queue, 'p, 'd, T, D>>,
}

/// A rejected state transition with its exact source owner.
#[repr(transparent)]
pub struct Failure<T, E>(Rejected<egress::NonEmpty<T>, E>);

impl<T, E> Failure<T, E> {
    const fn new(value: egress::NonEmpty<T>, cause: E) -> Self {
        Self(Rejected::new(value, cause))
    }

    pub fn into_parts(self) -> (egress::NonEmpty<T>, E) {
        self.0.into_parts()
    }
}

impl<'queue, 'token, 'p, 'd, T, D: demand::Mode> Reserve<'queue, 'token, 'p, 'd, T, D> {
    pub(super) fn new(
        node: pool::Vacant<'queue, 'p, 'd, T, egress::NonEmpty<T>, metadata::Local, D>,
        token: &'token mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            parts: Guard::new(token, pool::Prepared::from(node)),
        }
    }

    fn publish(self) {
        let (token, node) = self.parts.into_parts();
        node.commit(token);
    }

    pub fn rollback(self) -> egress::NonEmpty<T> {
        let (token, node) = self.parts.into_parts();
        node.rollback(token)
    }
}

impl<'p, 'd, T: data::raw::Resident<'d>, D: demand::Mode> Reserve<'_, '_, 'p, 'd, T, D> {
    /// Publishes the pre-owned payload only after the state transition succeeds.
    pub fn commit<R>(
        mut self,
        f: impl for<'a> FnOnce(&'a mut region::Token<region::Identity<'d>>) -> R,
    ) -> R {
        let output = f(self.parts.parts_mut().0);
        self.publish();
        output
    }

    /// Rolls the reservation back when a state transition is rejected.
    pub fn try_commit<R, E>(
        self,
        f: impl for<'a> FnOnce(&'a mut region::Token<region::Identity<'d>>) -> Result<R, E>,
    ) -> Result<R, Failure<T, E>> {
        self.try_with(|token, _| f(token))
    }

    /// Borrows the source while retaining it when a state transition is rejected.
    pub fn try_with<R, E>(
        mut self,
        f: impl for<'a> FnOnce(&'a mut region::Token<region::Identity<'d>>, &T) -> Result<R, E>,
    ) -> Result<R, Failure<T, E>> {
        let (token, node) = self.parts.parts_mut();
        let output = match f(token, node.value()) {
            Ok(output) => output,
            Err(cause) => return Err(Failure::new(self.rollback(), cause)),
        };
        self.publish();
        Ok(output)
    }
}

const fn layout<'queue, 'token, 'p: 'queue, 'd: 'p + 'token>() {
    assert!(
        std::mem::size_of::<Reserve<'queue, 'token, 'p, 'd, ()>>()
            == std::mem::size_of::<pool::Prepared<'queue, 'p, 'd, (), demand::Fallible>>()
                + std::mem::size_of::<&'token mut region::Token<region::Identity<'d>>>()
    );
    assert!(
        std::mem::size_of::<Reserve<'queue, 'token, 'p, 'd, ()>>()
            == 8 * std::mem::size_of::<usize>()
    );
    assert!(
        std::mem::align_of::<Reserve<'queue, 'token, 'p, 'd, ()>>()
            == std::mem::align_of::<pool::Prepared<'queue, 'p, 'd, (), demand::Fallible>>()
    );
}

const _: () = layout();
