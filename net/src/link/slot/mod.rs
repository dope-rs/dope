mod io;
pub(in crate::link) mod reception;
pub mod send;
pub mod types;

pub use io::Io;

pub enum Decision<C> {
    Drop,
    Close,
    Overrun { needs_rearm: bool },
    NoChunk { needs_rearm: bool },
    Discarded { needs_rearm: bool },
    Chunk { chunk: C, needs_rearm: bool },
}
