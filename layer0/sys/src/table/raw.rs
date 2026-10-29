use std::os::fd;

use crate::table::change;

/// # Safety
/// Layout yields `(capacity, accepted_prefix)`; the prefix fits capacity. Owned conversion preserves identity.
/// Accepted conversion succeeds only in its prefix and stays affine; `excludes` reports indices outside capacity.
pub unsafe trait Slots: change::raw::Slots {
    type Layout: Copy + Into<(Self::Capacity, Self::Capacity)>;
    type Owned: Copy + Into<Self::Physical>;
    type Accepted: TryFrom<(Self::Index, Self::Capacity)>;
    type Missing: Copy + Eq;

    fn excludes(capacity: Self::Capacity, index: Self::Index) -> Option<Self::Missing>;
    fn backing(index: Self::Index) -> Self::Missing;
    fn vacant(index: Self::Index) -> Self::Missing;
    fn replaced(index: Self::Index, expected: fd::RawFd, actual: fd::RawFd) -> Self::Missing;
}
