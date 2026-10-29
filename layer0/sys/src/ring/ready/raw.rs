use crate::{
    memory::buffer::provided,
    ring::ready::{Error, Ready},
};

type Failure<R> = Error<
    <R as Ready>::Ring,
    <R as Ready>::Producer,
    <<R as Ready>::Producer as provided::Producer>::Error,
    <R as Ready>::Registration,
    <R as Ready>::Registered,
    <R as Ready>::BufferError,
>;

pub trait Create: Ready {
    /// # Safety
    /// Keep this exact Context live until the reactor and every returned owner close.
    unsafe fn new(ring: Self::Ring, context: &Self::Context) -> Result<Self, Failure<Self>>;
}

/// Constructs and registers receive storage, retaining every failed resource.
pub trait Bind<P>: Ready {
    type Cleanup: crate::memory::Cleanup<P>;

    /// # Safety
    /// Keep the returned Context beside this reactor until native quiescence and
    /// destruction. Every failed registration retains its own exact Context.
    unsafe fn bind(
        ring: Self::Ring,
        layout: Self::Layout,
    ) -> Result<(Self, Self::Context), Result<P, Self::Cleanup>>;
}
