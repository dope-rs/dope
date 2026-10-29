use std::num;

use o3::base::num::Generation;

use crate::{
    driver,
    driver::schedule::{ready, ready::key::Stored},
};

/// Generation-exact stored key branded to one driver scope.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
#[doc(hidden)]
pub struct Retained<'d>(pub(in crate::driver::schedule::ready) ready::Key<'d>);

impl<'d> Retained<'d> {
    pub const fn slot(&self) -> num::NonZero<u16> {
        self.0.stored.index_code()
    }

    pub fn generation(&self) -> Generation {
        self.0.stored.epoch()
    }

    /// Wakes this generation through its exact driver owner.
    pub fn wake(self, owner: driver::Reference<'d>) {
        let access = ready::Access::new(&owner);
        owner.ready().arena().stored().complete(access, self);
    }

    pub(in crate::driver::schedule::ready) const fn new(key: ready::Key<'d>) -> Self {
        Self(key)
    }

    pub(in crate::driver::schedule) const fn raw(self) -> Stored {
        self.0.raw()
    }
}
