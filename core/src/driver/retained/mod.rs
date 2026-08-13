//! Driver access scoped to one exact retained owner.
//!
//! An ordinary [`driver::Context`] can submit operations whose arguments are
//! copied by the backend. Only [`Context`] can submit an operation which keeps
//! a pointer into owner-backed storage after the call returns.
//!
//! Erasure and submission are one operation. The erased intermediate is
//! private to `dope-core`, so it cannot escape, be stored, or be replayed with
//! a different owner's authority.
//!
//! ```compile_fail
//! use dope_core::{
//!     driver::{self, retained, route::KeyTag},
//! };
//!
//! unsafe fn bypass<'d>(
//!     driver: &mut driver::Context<'_, 'd>,
//!     slots: &driver::flight::Slots<'d, KeyTag<1>>,
//!     submission: retained::raw::Submission<'_, 'd, KeyTag<1>>,
//! ) {
//!     retained::raw::Owner::submit(driver, slots, submission).unwrap();
//! }
//! ```
//!
//! An owner lifetime may be shortened for a poll borrow, but never widened.
//!
//! ```compile_fail
//! use dope_core::driver::retained::Context;
//!
//! fn widen<'a, 'short>(
//!     context: Context<'a, 'short, 'static>,
//! ) -> Context<'a, 'static, 'static> {
//!     context
//! }
//! ```

use std::{marker, ops};

use crate::{
    backend::{self, bound},
    driver::{self, flight},
    platform::reactor,
};

pub mod raw;

/// Driver access carrying one exact retained owner's zero-sized proof, with
/// the same representation as [`driver::Context`].
#[doc(hidden)]
#[repr(transparent)]
pub struct Context<'a, 'owner, 'd: 'owner> {
    driver: driver::Context<'a, 'd>,
    _owner: marker::PhantomData<raw::Owner<'owner, 'd>>,
}

impl<'a, 'owner, 'd: 'owner> Context<'a, 'owner, 'd> {
    /// Joins driver access with a validated owner proof.
    #[doc(hidden)]
    pub fn new(driver: driver::Context<'a, 'd>, owner: raw::Owner<'owner, 'd>) -> Self {
        let _ = owner;
        Self {
            driver,
            _owner: marker::PhantomData,
        }
    }

    pub fn reborrow(&mut self) -> Context<'_, 'owner, 'd> {
        Context {
            driver: self.driver.reborrow(),
            _owner: marker::PhantomData,
        }
    }

    pub fn driver(&mut self) -> &mut driver::Context<'a, 'd> {
        &mut self.driver
    }
}

impl<'a, 'owner, 'd: 'owner> Context<'a, 'owner, 'd> {
    pub(in crate::driver) fn submit_bound(
        &mut self,
        submission: bound::Bound<'owner, 'd>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>
    where
        backend::Backend: reactor::Source,
    {
        let mut queue = reactor::Source::queue(self.backend());
        reactor::Queue::submit(&mut queue, submission)
    }
}

impl<'a, 'owner, 'd: 'owner> ops::Deref for Context<'a, 'owner, 'd> {
    type Target = driver::Context<'a, 'd>;

    fn deref(&self) -> &Self::Target {
        &self.driver
    }
}

impl<'a, 'owner, 'd: 'owner> ops::DerefMut for Context<'a, 'owner, 'd> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.driver
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Context<'static, 'static, 'static>>()
            == std::mem::size_of::<driver::Context<'static, 'static>>()
    );
    assert!(
        std::mem::align_of::<Context<'static, 'static, 'static>>()
            == std::mem::align_of::<driver::Context<'static, 'static>>()
    );
};
