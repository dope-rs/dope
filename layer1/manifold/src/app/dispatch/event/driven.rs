use std::marker;

/// Driver-backed completion capability with statically selected authority.
pub struct Driven<A>(marker::PhantomData<fn() -> A>);
