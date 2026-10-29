use std::{marker::PhantomData, num::NonZero};

use dope_sys::table::permit;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Key(pub u64);

// SAFETY: the wrapped scalar is the complete generation identity.
unsafe impl permit::raw::Key for Key {
    fn identity(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy)]
pub struct Lifetime<'a>(NonZero<u64>, PhantomData<fn(&'a ()) -> &'a ()>);

// SAFETY: the retained nonzero scalar is the complete identity, without callbacks.
unsafe impl permit::raw::Key for Lifetime<'_> {
    fn identity(self) -> u64 {
        self.0.get()
    }
}
