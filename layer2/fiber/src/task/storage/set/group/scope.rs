use core::marker;

/// The invariant identity of one scoped task allocation.
pub struct Scope<'s, D> {
    scope: marker::PhantomData<fn(&'s mut ()) -> &'s mut ()>,
    domain: marker::PhantomData<fn(D) -> D>,
}
