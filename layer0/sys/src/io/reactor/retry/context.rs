use crate::{Errno, memory, table, table::change};

/// The exact byte-count and completion types retained by the driver.
pub trait Context {
    type Slots: table::raw::Slots;
    type Key: Copy + Eq + Into<usize> + Into<<Self::Slots as change::raw::Slots>::Data>;
    type Operation: Copy;
    type Extent: memory::raw::Extent;
    type Outcome;
    type Unit;

    const SOCKET_ERROR: Self::Operation;
    const SEND: Self::Operation;
    const CONNECT: Self::Operation;

    fn sent(result: Result<<Self::Extent as memory::raw::Extent>::Length, Errno>) -> Self::Outcome;
    fn connected(result: Result<(), Errno>) -> Self::Unit;
}
