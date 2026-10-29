use crate::memory;

/// Failed removal retains the same fixed-file authority through every retry.
pub trait Release<P>: memory::Cleanup<(P, Self::Target)> {
    type Ring;
    type File;
    type Session;
    type Target: Copy;

    fn raw(ring: Self::Ring, file: Self::File, primary: P) -> Result<(P, Self::Target), Self>;
    fn session(session: Self::Session, primary: P) -> Result<(P, Self::Target), Self>;
}
