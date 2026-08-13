use o3::collections::slab;

use crate::driver::route::{self, table};

/// A non-empty target-table capacity with one encodable slot left as a sentinel.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct ConnectionCapacity(route::SlotIndex);

impl ConnectionCapacity {
    pub const fn new(value: usize) -> Option<Self> {
        match table::Capacity::new(value) {
            Some(capacity) if capacity.get() != 0 => {
                match route::SlotIndex::try_new(capacity.raw()) {
                    Some(sentinel) => Some(Self(sentinel)),
                    None => None,
                }
            }
            Some(_) | None => None,
        }
    }

    pub const fn get(self) -> usize {
        self.0.raw() as usize
    }

    pub const fn raw(self) -> u32 {
        self.0.raw()
    }

    pub const fn table(self) -> table::Capacity {
        table::Capacity(slab::Capacity::new(self.0.raw()))
    }

    pub const fn sentinel(self) -> route::SlotIndex {
        self.0
    }
}
