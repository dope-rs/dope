//! Reply arena capacity and per-reply limits.

use std::{mem, num::NonZero};

use o3::depot::capacity;

use crate::reply::{retain::grant, usage};

mod domain;
mod position;
pub mod raw;

pub use domain::Domain;
pub use position::Position;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capacity {
    lanes: Lanes,
    entries: Entries,
    data: Data,
    bytes: usage::Bytes,
    values: usage::Values,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Lanes(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Entries(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limit {
    data: Data,
    bytes: usage::Bytes,
    values: usage::Values,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Data(usize);

pub const MAXIMUM_ENTRIES: Entries = Entries(u32::MAX as usize / 2);
pub const MAXIMUM_DATA: Data = Data(u32::MAX as usize);

impl Data {
    pub const fn new(items: usize) -> Self {
        Self(items)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

impl capacity::Extent for Data {
    fn get(self) -> usize {
        self.0
    }
}

impl Lanes {
    pub const fn new(lanes: usize) -> Self {
        Self(lanes)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

impl Entries {
    pub const fn new(entries: usize) -> Self {
        Self(entries)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

impl Capacity {
    pub const fn new(
        lanes: Lanes,
        entries: Entries,
        data: Data,
        bytes: usage::Bytes,
        values: usage::Values,
    ) -> Self {
        Self {
            lanes,
            entries,
            data,
            bytes,
            values,
        }
    }
}

impl Limit {
    pub const fn new(data: Data, bytes: usage::Bytes, values: usage::Values) -> Self {
        Self {
            data,
            bytes,
            values,
        }
    }

    pub const fn data(self) -> Data {
        self.data
    }

    pub const fn bytes(self) -> usage::Bytes {
        self.bytes
    }

    pub const fn values(self) -> usage::Values {
        self.values
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Layout {
    lanes: NonZero<u32>,
    entries: NonZero<u32>,
    data: Data,
    limit: Limit,
    admission: grant::Plan<2>,
    payload: grant::Plan<3>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    ZeroLanes,
    LanesTooLarge {
        actual: Lanes,
        maximum: Lanes,
    },
    ZeroEntries,
    ZeroBytes,
    ZeroValues,
    ZeroDataLimit,
    ZeroByteLimit,
    ZeroValueLimit,
    EntriesBelowLanes {
        entries: Entries,
        lanes: Lanes,
    },
    EntriesTooLarge {
        actual: Entries,
        maximum: Entries,
    },
    DataTooLarge {
        actual: Data,
        maximum: Data,
    },
    DataLimitUnattainable {
        requested: Data,
        available: Data,
    },
    ByteLimitUnattainable {
        requested: usage::Bytes,
        available: usage::Bytes,
    },
    ValueLimitUnattainable {
        requested: usage::Values,
        available: usage::Values,
    },
}

impl Layout {
    pub fn new(capacity: Capacity, limit: Limit) -> Result<Self, Error> {
        Self::with_limit(capacity, Some(limit))
    }

    /// Uses the largest per-reply limits attainable by one lane.
    pub fn bounded(capacity: Capacity) -> Result<Self, Error> {
        Self::with_limit(capacity, None)
    }

    fn with_limit(capacity: Capacity, wanted: Option<Limit>) -> Result<Self, Error> {
        let lanes = capacity::Count::try_from(capacity.lanes.get())
            .map_err(|cause| Error::LanesTooLarge {
                actual: Lanes::new(cause.requested()),
                maximum: Lanes::new(u32::MAX as usize),
            })?
            .nonzero()
            .ok_or(Error::ZeroLanes)?;
        let entries = capacity::Count::try_from(capacity.entries.get())
            .map_err(|cause| Error::EntriesTooLarge {
                actual: Entries::new(cause.requested()),
                maximum: MAXIMUM_ENTRIES,
            })?
            .nonzero()
            .ok_or(Error::ZeroEntries)?;
        if entries < lanes {
            return Err(Error::EntriesBelowLanes {
                entries: capacity.entries,
                lanes: capacity.lanes,
            });
        }
        if capacity.entries.get() > MAXIMUM_ENTRIES.get() {
            return Err(Error::EntriesTooLarge {
                actual: capacity.entries,
                maximum: MAXIMUM_ENTRIES,
            });
        }
        if capacity.data.get() > MAXIMUM_DATA.get() {
            return Err(Error::DataTooLarge {
                actual: capacity.data,
                maximum: MAXIMUM_DATA,
            });
        }
        if capacity.bytes.get() == 0 {
            return Err(Error::ZeroBytes);
        }
        if capacity.values.get() == 0 {
            return Err(Error::ZeroValues);
        }
        if let Some(limit) = wanted {
            if limit.data().get() == 0 {
                return Err(Error::ZeroDataLimit);
            }
            if limit.bytes().get() == 0 {
                return Err(Error::ZeroByteLimit);
            }
            if limit.values().get() == 0 {
                return Err(Error::ZeroValueLimit);
            }
        } else if capacity.data.get() == 0 {
            return Err(Error::ZeroDataLimit);
        }
        let fair = NonZero::<usize>::MIN.saturating_add(lanes.get() as usize - 1);
        let admit = grant::Plan::slots(entries, lanes);
        let payload = grant::Plan::payload(
            [
                capacity.data.get(),
                capacity.values.get(),
                capacity.bytes.get(),
            ],
            fair,
        );
        let ceiling = payload.ceiling();
        let limit = match wanted {
            Some(limit) => {
                if limit.data().get() > ceiling[0] {
                    return Err(Error::DataLimitUnattainable {
                        requested: limit.data(),
                        available: Data::new(ceiling[0]),
                    });
                }
                if limit.values().get() > ceiling[1] {
                    return Err(Error::ValueLimitUnattainable {
                        requested: limit.values(),
                        available: usage::Values::new(ceiling[1]),
                    });
                }
                if limit.bytes().get() > ceiling[2] {
                    return Err(Error::ByteLimitUnattainable {
                        requested: limit.bytes(),
                        available: usage::Bytes::new(ceiling[2]),
                    });
                }
                limit
            }
            None => Limit::new(
                Data::new(ceiling[0]),
                usage::Bytes::new(ceiling[2]),
                usage::Values::new(ceiling[1]),
            ),
        };
        Ok(Self {
            lanes,
            entries,
            data: capacity.data,
            limit,
            admission: admit,
            payload,
        })
    }

    pub fn single(capacity: Entries, limit: Limit) -> Result<Self, Error> {
        Self::new(
            Capacity::new(
                Lanes::new(1),
                capacity,
                limit.data,
                limit.bytes,
                limit.values,
            ),
            limit,
        )
    }

    pub const fn lane_count(self) -> NonZero<u32> {
        self.lanes
    }

    /// Maximum outstanding orders attainable by one lane.
    pub const fn order_limit(self) -> NonZero<u32> {
        self.admission.order_limit()
    }

    pub const fn limit(self) -> Limit {
        self.limit
    }

    pub(super) const fn entries(self) -> NonZero<u32> {
        self.entries
    }

    pub(super) const fn data(self) -> Data {
        self.data
    }

    pub(super) const fn admission(self) -> grant::Plan<2> {
        self.admission
    }

    pub(super) const fn payload(self) -> grant::Plan<3> {
        self.payload
    }
}

const _: () = assert!(mem::size_of::<Error>() == 3 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Result<Layout, Error>>() == mem::size_of::<Layout>());
