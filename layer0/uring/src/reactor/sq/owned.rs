use std::{marker, mem};

use crate::reactor::sq;

/// An SQE whose encoded fields do not borrow caller-owned memory.
#[repr(transparent)]
pub struct Owned<T>(sq::Entry, marker::PhantomData<fn(T) -> T>);

impl<T> Owned<T> {
    pub(crate) const fn new(entry: sq::Entry) -> Self {
        Self(entry, marker::PhantomData)
    }
}

const _: () = assert!(mem::size_of::<Owned<()>>() == mem::size_of::<sq::Entry>());
