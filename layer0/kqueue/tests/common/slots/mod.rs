pub mod inputs;

use std::{num::NonZero, os::fd};

use dope_sys::{table, table::change};
use o3::base::num::bounded;

use crate::common::slots::inputs::{Accepted, Capacity, Layout, Missing, Owned, Physical};

#[derive(Debug)]
pub struct Slots;

// SAFETY: both domains retain their bounded indices. The capacity iterator
// enumerates precisely the ascending prefix, with no fabricated slot values.
unsafe impl change::raw::Slots for Slots {
    type Index = Index;
    type Physical = Physical;
    type Capacity = Capacity;
    type Data = NonZero<usize>;

    const MAX: usize = 4;

    fn offset(index: Index) -> usize {
        index.get() as usize
    }
    fn count(capacity: Capacity) -> usize {
        capacity.0.get() as usize
    }
    unsafe fn nth(_: Capacity, ordinal: usize) -> Index {
        // SAFETY: the caller passes an ordinal below `count(capacity)`, so it names
        // one of the four indices the bounded domain admits.
        unsafe { bounded::raw::Proof::assume(ordinal as u32) }.retain()
    }
}

// SAFETY: the fixed accepted prefix is one of the table's two rows. Accepted
// construction validates that prefix and its token deliberately is not Copy.
unsafe impl table::raw::Slots for Slots {
    type Layout = Layout;
    type Owned = Owned;
    type Accepted = Accepted;
    type Missing = Missing;

    fn excludes(capacity: Capacity, index: Index) -> Option<Missing> {
        (index.get() >= capacity.0.get()).then_some(Missing::Bounds(index))
    }
    fn backing(index: Index) -> Missing {
        Missing::Backing { index }
    }
    fn vacant(index: Index) -> Missing {
        Missing::Vacant { index }
    }

    fn replaced(index: Index, expected: fd::RawFd, actual: fd::RawFd) -> Missing {
        Missing::Replaced {
            index,
            expected,
            actual,
        }
    }
}

type Index = bounded::U32<0, 3>;
