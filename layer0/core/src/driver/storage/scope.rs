use o3::buffer;

use crate::driver::storage;

/// Arenas retained through the complete driver generation and its reclamation.
#[repr(transparent)]
pub struct Scope<A>(A);

impl<A: buffer::raw::Arenas> Scope<A> {
    pub const fn new(arenas: A) -> Self {
        Self(arenas)
    }

    pub(in crate::driver) fn into_inner(self) -> A {
        self.0
    }
}

impl<A: buffer::raw::Arenas> storage::Plan for Scope<A> {
    type Output<'p, 'd>
        = A::Roots<'d>
    where
        Self: 'd,
        'd: 'p;
}
