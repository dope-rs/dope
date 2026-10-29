use std::{array::IntoIter, iter::Take, os::fd};

use o3::base::num::bounded;

type Index = bounded::U32<0, 3>;
#[derive(Clone, Copy, Debug)]
pub struct Capacity(pub(super) bounded::U32<0, 4>);

impl IntoIterator for Capacity {
    type Item = Index;
    type IntoIter = Take<IntoIter<Index, 4>>;

    fn into_iter(self) -> Self::IntoIter {
        [
            Index::exact::<0>(),
            Index::exact::<1>(),
            Index::exact::<2>(),
            Index::exact::<3>(),
        ]
        .into_iter()
        .take(self.0.get() as usize)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Owned(pub(super) Physical);

impl From<Physical> for Owned {
    fn from(slot: Physical) -> Self {
        Self(slot)
    }
}

#[derive(Debug)]
pub struct Accepted(Physical);

impl Accepted {
    pub fn physical(&self) -> Physical {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Missing {
    Bounds(Index),
    Backing {
        index: Index,
    },
    Vacant {
        index: Index,
    },
    Replaced {
        index: Index,
        expected: fd::RawFd,
        actual: fd::RawFd,
    },
}

impl TryFrom<(Index, Capacity)> for Accepted {
    type Error = (Index, Capacity);

    fn try_from((index, capacity): (Index, Capacity)) -> Result<Self, Self::Error> {
        if index.get() < capacity.0.get() {
            Ok(Self(Physical(index)))
        } else {
            Err((index, capacity))
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Layout;

impl From<Layout> for (Capacity, Capacity) {
    fn from(_: Layout) -> Self {
        (
            Capacity(bounded::U32::exact::<2>()),
            Capacity(bounded::U32::exact::<1>()),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Physical(pub(super) Index);

impl Physical {
    pub fn at<const VALUE: u32>() -> Self {
        Self(Index::exact::<VALUE>())
    }

    pub fn index(self) -> Index {
        self.0
    }
}

impl From<Physical> for Index {
    fn from(slot: Physical) -> Self {
        slot.0
    }
}

impl From<Physical> for u32 {
    fn from(slot: Physical) -> Self {
        slot.0.get()
    }
}

impl From<Owned> for Physical {
    fn from(slot: Owned) -> Self {
        slot.0
    }
}
