use std::marker;

use dope_core::driver::lifecycle::quiesce;

use crate::executor;

/// Proof that shutdown visited every owner beneath one exact root.
#[must_use]
pub struct Pending<'app, 'd: 'app, D> {
    _app: marker::PhantomData<*mut &'app ()>,
    _driver: marker::PhantomData<*mut &'d ()>,
    _owner: marker::PhantomData<*mut D>,
    _thread: o3::ThreadBound,
}

impl<'app, 'd: 'app, D> Pending<'app, 'd, D> {
    pub(super) fn new() -> Self {
        Self {
            _app: marker::PhantomData,
            _driver: marker::PhantomData,
            _owner: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    pub(crate) fn finish<'a>(
        self,
        finalization: quiesce::Final<'a, 'd>,
    ) -> executor::raw::FinishRoot<'a, 'app, 'd, D> {
        executor::raw::FinishRoot::new(finalization)
    }
}

const _: () = assert!(std::mem::size_of::<Pending<'static, 'static, ()>>() == 0);
