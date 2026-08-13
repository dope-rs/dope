//! Exact application-root finalization authority.

use std::marker;

use dope_core::driver::{self, lifecycle};
use lifecycle::quiesce;

/// Exact-root authority to finalize resources after driver quiescence.
#[must_use]
pub struct FinishRoot<'a, 'app, 'd: 'app, D> {
    finalization: lifecycle::Finalize<'a, 'd>,
    _app: marker::PhantomData<*mut &'app ()>,
    _driver: marker::PhantomData<*mut &'d ()>,
    _owner: marker::PhantomData<*mut D>,
    _thread: o3::ThreadBound,
}

impl<'a, 'app, 'd: 'app, D> FinishRoot<'a, 'app, 'd, D> {
    pub(in crate::executor::raw) fn new(finalization: quiesce::Final<'a, 'd>) -> Self {
        Self {
            finalization: lifecycle::Finalize::new(finalization),
            _app: marker::PhantomData,
            _driver: marker::PhantomData,
            _owner: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    pub fn context(&mut self) -> lifecycle::Finalize<'_, 'd> {
        self.finalization.reborrow()
    }
}

const _: () = assert!(
    std::mem::size_of::<FinishRoot<'static, 'static, 'static, ()>>()
        == std::mem::size_of::<driver::Context<'static, 'static>>()
);
