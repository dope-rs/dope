//! Logical reply-resource accounting and construction.

use std::mem;

use crate::reply::order;

/// Logical bytes and parsed values retained by one reply value.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Usage {
    bytes: Bytes,
    values: Values,
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Bytes(usize);

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Values(usize);

/// Amount held before a reply-resource insertion.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Current(usize);

impl Current {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

/// Amount introduced by a reply-resource insertion.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Added(usize);

impl Added {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

/// Fixed upper bound for one reply resource.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Limit(usize);

impl Limit {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

/// Amount required by a live-credit admission.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Required(usize);

impl Required {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

/// Amount available from live credits during an admission attempt.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Available(usize);

impl Available {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

impl Bytes {
    pub const ZERO: Self = Self(0);

    pub const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    pub const fn get(self) -> usize {
        self.0
    }

    fn checked_add(self, added: Self) -> Option<Self> {
        Some(Self(self.0.checked_add(added.0)?))
    }
}

impl Values {
    pub const ZERO: Self = Self(0);

    pub const fn new(values: usize) -> Self {
        Self(values)
    }

    pub const fn get(self) -> usize {
        self.0
    }

    fn checked_add(self, added: Self) -> Option<Self> {
        Some(Self(self.0.checked_add(added.0)?))
    }
}

impl Usage {
    pub const ZERO: Self = Self {
        bytes: Bytes::ZERO,
        values: Values::ZERO,
    };

    pub const fn new(bytes: Bytes, values: Values) -> Self {
        Self { bytes, values }
    }

    pub const fn bytes(self) -> Bytes {
        self.bytes
    }

    pub const fn values(self) -> Values {
        self.values
    }

    pub(in crate::reply) fn try_add(self, other: Self) -> Result<Self, Capacity> {
        let Some(bytes) = self.bytes.checked_add(other.bytes) else {
            return Err(Capacity::Arithmetic {
                resource: Resource::Bytes,
                current: Current::new(self.bytes.get()),
                added: Added::new(other.bytes.get()),
            });
        };
        let Some(values) = self.values.checked_add(other.values) else {
            return Err(Capacity::Arithmetic {
                resource: Resource::Values,
                current: Current::new(self.values.get()),
                added: Added::new(other.values.get()),
            });
        };
        Ok(Self { bytes, values })
    }
}

/// One independently limited reply resource.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Resource {
    Items,
    Values,
    Bytes,
}

/// The exact capacity condition that terminally rejected one reply insertion.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Capacity {
    Arithmetic {
        resource: Resource,
        current: Current,
        added: Added,
    },
    Limit {
        resource: Resource,
        current: Current,
        added: Added,
        limit: Limit,
    },
    /// A collected reply cannot wait for these live shared credits.
    Unavailable(Backpressure),
}

/// The exact live-credit condition that denied an insertion.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Backpressure {
    resource: Resource,
    required: Required,
    available: Available,
}

impl Backpressure {
    pub(in crate::reply) const fn new(
        resource: Resource,
        required: Required,
        avail: Available,
    ) -> Self {
        Self {
            resource,
            required,
            available: avail,
        }
    }

    pub const fn into_parts(self) -> (Resource, Required, Available) {
        (self.resource, self.required, self.available)
    }
}

const _: () = {
    assert!(mem::size_of::<Current>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Added>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Limit>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Required>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Available>() == mem::size_of::<usize>());
    assert!(mem::size_of::<Backpressure>() == 3 * mem::size_of::<usize>());
};

/// Reports the logical resources retained by a reply value.
pub trait Footprint {
    fn footprint(&self) -> Usage;
}

/// Describes a reply value before constructing its owned representation.
pub trait Build<C> {
    type Error;

    fn usage(&self) -> Usage;

    fn try_build(self) -> Result<C, (Self, Self::Error)>
    where
        Self: Sized;
}

pub enum PushError<B, E, C> {
    /// Global credits are live elsewhere; this untouched builder is retryable.
    Backpressure {
        builder: B,
        cause: Backpressure,
    },
    /// The front reply cannot admit this value under its acceptance policy.
    Capacity {
        builder: B,
        cause: Capacity,
    },
    /// The protocol lane has no data-capable reply at its front.
    Front {
        builder: B,
        front: order::Front,
    },
    Build {
        builder: B,
        error: E,
    },
    Footprint {
        value: C,
        declared: Usage,
        actual: Usage,
    },
}

pub enum InsertError<C> {
    Backpressure { value: C, cause: Backpressure },
    Capacity { value: C, cause: Capacity },
    Front { value: C, front: order::Front },
}
