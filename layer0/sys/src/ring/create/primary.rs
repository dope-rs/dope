use crate::memory;

/// The exact construction failure survives every later cleanup attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Primary<M, R, B> {
    Ring(M),
    Memory(memory::Error),
    Register(R),
    Provided(B),
}

impl<M, R, B> From<memory::Error> for Primary<M, R, B> {
    fn from(error: memory::Error) -> Self {
        Self::Memory(error)
    }
}
