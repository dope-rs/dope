pub mod raw;

use std::io;

use crate::{
    backend, driver,
    driver::ops::{reactors, retirements},
    platform,
};

#[doc(hidden)]
pub struct Lease {
    _owner: raw::Owner,
}

#[doc(hidden)]
#[must_use]
pub struct Final<'a, 'd> {
    context: driver::Context<'a, 'd>,
}

impl Lease {
    pub fn new(owner: raw::Owner) -> Self {
        Self { _owner: owner }
    }
}

impl<'a, 'd> Final<'a, 'd> {
    pub(in crate::driver) fn new(mut context: driver::Context<'a, 'd>) -> io::Result<Self> {
        reactors::Returned::reclaim_all(&mut context);
        retirements::Reclaimer::<true>::new(&mut context).all();
        let (backend, drain) = context.backend_drain();
        <backend::Backend as platform::Quiesce>::all(backend, drain)?;
        Ok(Self { context })
    }

    pub(crate) fn context(&mut self) -> &mut driver::Context<'a, 'd> {
        &mut self.context
    }

    pub(crate) fn reborrow(&mut self) -> Final<'_, 'd> {
        Final {
            context: self.context.reborrow(),
        }
    }
}
