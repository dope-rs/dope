use crate::table::registry::Entry;

/// Exclusively owns the exact registered read through infallible removal.
pub trait Occupied: Sized {
    type Entry: Entry;

    fn get(&self) -> &Self::Entry;
    fn remove(self) -> Self::Entry;
}
