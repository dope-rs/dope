use crate::{table, table::change};

/// Typed read identity and address-space projections retained by one driver.
pub trait Context: Copy {
    type Slots: table::raw::Slots<Physical: Into<u32>>;
    type Key: Copy + Eq + Into<usize> + Into<<Self::Slots as change::raw::Slots>::Data>;
    type Flow: Copy + Eq;
    type Operation: Copy;

    const ACCEPT: Self::Operation;
    const RECV: Self::Operation;
    const MESSAGE: Self::Operation;
}
