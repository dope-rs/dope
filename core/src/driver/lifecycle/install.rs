use std::marker;

use crate::driver::lifecycle;

/// Proof that nested resources are installed beneath their pinned owner.
#[must_use]
pub struct Install<'a, 'd> {
    _borrow: marker::PhantomData<&'a mut ()>,
    _driver: marker::PhantomData<*mut &'d ()>,
    _thread: o3::ThreadBound,
}

impl<'a, 'd> Install<'a, 'd> {
    pub fn new<T: lifecycle::raw::InstallRoot + ?Sized>(_root: &'a mut T) -> Self {
        Self {
            _borrow: marker::PhantomData,
            _driver: marker::PhantomData,
            _thread: o3::ThreadBound::NEW,
        }
    }
}

const _: () = assert!(std::mem::size_of::<Install<'static, 'static>>() == 0);
