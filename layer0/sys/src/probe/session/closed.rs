use crate::ring::map::Unmap;

/// Failed cleanup retains the fixed-file authority until every mapping closes.
pub trait Closed: Unmap {
    type Target;

    fn slot(&self) -> Self::Target;
}
