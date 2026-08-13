//! Exact pinned-root installation authority.

use std::{marker, pin};

use dope_core::driver::lifecycle;
use o3::cell::brand;

/// Proof that an exact application root is pinned under runtime ownership.
#[must_use]
pub struct InstallRoot<'app, 'd: 'app, D> {
    _app: marker::PhantomData<*mut &'app ()>,
    _driver: marker::PhantomData<*mut &'d ()>,
    _owner: marker::PhantomData<*mut D>,
    _thread: o3::ThreadBound,
}

unsafe impl<D> lifecycle::raw::InstallRoot for InstallRoot<'_, '_, D> {}

impl<'app, 'd: 'app, D> InstallRoot<'app, 'd, D> {
    /// # Safety
    /// `app` must be the exact pinned application before it can be driven.
    pub(crate) unsafe fn new(_app: pin::Pin<&'app brand::Value<'d, D>>) -> Self {
        Self {
            _app: marker::PhantomData,
            _driver: marker::PhantomData,
            _owner: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }

    pub fn context(&mut self) -> lifecycle::Install<'_, 'd> {
        lifecycle::Install::new(self)
    }
}

const _: () = assert!(std::mem::size_of::<InstallRoot<'static, 'static, ()>>() == 0);
