type Invariant<'a, T> = marker::PhantomData<(&'a mut (), fn(T) -> T)>;

use std::{marker, mem};

use crate::reactor::sq;

#[repr(transparent)]
pub struct Submit<'a, T>(sq::Entry, Invariant<'a, T>);

impl<T> Submit<'_, T> {
    pub(crate) const fn new(entry: sq::Entry) -> Self {
        Self(entry, marker::PhantomData)
    }

    pub(crate) fn flags(mut self, flags: sq::Flags) -> Self {
        self.0 = self.0.flags(flags);
        self
    }
}

const _: () = assert!(mem::size_of::<Submit<'_, ()>>() == mem::size_of::<sq::Entry>());
