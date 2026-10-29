use crate::memory;

/// An unbound construction result can acquire its payload owner exactly once.
pub trait Build<P>: Sized {
    type Context;
    type Cleanup: memory::Cleanup<P>;

    fn close(self, context: Self::Context) -> Result<P, Self::Cleanup>;
    fn into_result(self) -> Result<P, Self::Cleanup>;
}
