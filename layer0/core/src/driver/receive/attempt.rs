use std::{marker, mem};

use dope_sys::table::permit::Queue;

use crate::driver;

/// One linear permission to attempt a provided-buffer receive submission.
#[must_use = "a receive attempt must be committed or discarded"]
#[repr(transparent)]
pub struct Attempt<'d> {
    driver: marker::PhantomData<driver::Reference<'d>>,
    _permit: <crate::Permits as Queue>::Capability,
}

impl<'d, P> Attempt<'d>
where
    crate::Permits: Queue<Capability = P>,
{
    pub(in crate::driver::receive) const fn new(permit: P, _driver: driver::Reference<'d>) -> Self {
        Self {
            driver: marker::PhantomData,
            _permit: permit,
        }
    }

    /// Consumes this permission to attempt a receive submission.
    pub fn commit(self) {}
}

const _: () = {
    assert!(mem::size_of::<Attempt<'static>>() == 0);
    assert!(mem::align_of::<Attempt<'static>>() == 1);
};
