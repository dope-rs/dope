use crate::{memory, memory::buffer::provided};

/// Retains all ring and payload owners while construction cleanup is incomplete.
pub trait Retain<P>: memory::Cleanup<P> {
    type Build: super::Build<P, Context = Self::Context, Cleanup = Self>;
    type Ring;
    type Probe;
    type Canary;
    type Context: provided::Context<Cleanup = Self::Buffers>;
    type Buffers: provided::Cleanup<Context = Self::Context>;
    type Producer;
    type Registration;
    type Registered;
    type BufferError;

    fn probe(state: Self::Probe) -> Self;
    fn open<F: provided::Failure<Cleanup = Self::Buffers>>(
        ring: Self::Ring,
        fault: F,
    ) -> Self::Build;
    fn canary(ring: Self::Ring, primary: P, buffers: Self::Canary) -> Result<P, Self>;
    fn unmap(ring: Self::Ring, primary: P, buffers: Self::Buffers) -> Self;
    fn registration(state: Self::Registration, buffers: Self::Producer) -> Self::Build;
    fn provided(
        cause: Self::BufferError,
        ring: Self::Registered,
        buffers: Self::Producer,
    ) -> Self::Build;
}
