use std::num;

use o3::depot::capacity;

use crate::driver::route::table::conn;

/// Validated non-zero number of simultaneously resident task owners.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Capacity(num::NonZero<u32>);

impl Capacity {
    pub const fn new(owners: num::NonZero<u32>) -> Self {
        Self(owners)
    }

    pub const fn count(self) -> num::NonZero<u32> {
        self.0
    }
}

impl TryFrom<num::NonZero<usize>> for Capacity {
    type Error = super::Overflow;

    fn try_from(owners: num::NonZero<usize>) -> Result<Self, Self::Error> {
        Ok(Self(capacity::Count::try_nonzero(owners)?))
    }
}

impl From<conn::Capacity> for Capacity {
    fn from(owners: conn::Capacity) -> Self {
        Self::new(owners.count())
    }
}

const _: () = {
    assert!(std::mem::size_of::<Capacity>() == std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Option<Capacity>>() == std::mem::size_of::<u32>());
};
