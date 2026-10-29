use std::marker;

/// Scheduler-backed completion capability with statically selected authority.
pub struct Timed<A>(marker::PhantomData<fn() -> A>);
