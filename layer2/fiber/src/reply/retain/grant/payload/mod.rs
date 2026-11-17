//! Reply payload ownership.

use std::{mem::size_of, num};

use o3::mem;

use crate::reply::{usage, usage::amount::Amount};

pub(in crate::reply) mod raw;

/// One payload data unit, with no value or byte component.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Data;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Values(num::NonZero<usize>);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Bytes(num::NonZero<usize>);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Value<A = usage::Usage>(A);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) struct Failure<A = usage::Usage>(A);

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) enum Requested {
    Value(Value),
    Failure(Failure),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::reply) enum Used {
    Values { values: Values, bytes: usage::Bytes },
    Bytes(Bytes),
}

pub(in crate::reply::retain) enum Class {
    Zero,
    Values(Values),
    Bytes(Bytes),
    Both(Values, Bytes),
}

// SAFETY: `Data` represents exactly one payload data unit.
unsafe impl mem::raw::Amount<3, usize> for Data {
    fn get(self) -> [usize; 3] {
        [1, 0, 0]
    }
}

// SAFETY: `Values` stores its exact nonzero payload-value component.
unsafe impl mem::raw::Amount<3, usize> for Values {
    fn get(self) -> [usize; 3] {
        [0, self.0.get(), 0]
    }
}

// SAFETY: pool admission proves the componentwise sum is representable.
unsafe impl mem::raw::Sum<3, usize> for Values {
    unsafe fn add_raw(self, other: Self) -> Self {
        // SAFETY: the source-bound pair owns disjoint positive components;
        // their admitted lane total bounds this sum, so it neither wraps nor is zero.
        Self(unsafe { num::NonZero::new_unchecked(self.0.get().wrapping_add(other.0.get())) })
    }
}

// SAFETY: `Bytes` stores its exact nonzero payload-byte component.
unsafe impl mem::raw::Amount<3, usize> for Bytes {
    fn get(self) -> [usize; 3] {
        [0, 0, self.0.get()]
    }
}

// SAFETY: pool admission proves the componentwise sum is representable.
unsafe impl mem::raw::Sum<3, usize> for Bytes {
    unsafe fn add_raw(self, other: Self) -> Self {
        // SAFETY: the source-bound pair owns disjoint positive components;
        // their admitted lane total bounds this sum, so it neither wraps nor is zero.
        Self(unsafe { num::NonZero::new_unchecked(self.0.get().wrapping_add(other.0.get())) })
    }
}

impl<A: Amount> From<Value<A>> for Requested {
    fn from(value: Value<A>) -> Self {
        Self::Value(Value(value.usage()))
    }
}

impl From<Failure> for Requested {
    fn from(value: Failure) -> Self {
        Self::Failure(value)
    }
}

// SAFETY: `Value` fixes data at one and stores its exact usage components.
unsafe impl<A: Amount> mem::raw::Amount<3, usize> for Value<A> {
    fn get(self) -> [usize; 3] {
        let usage = self.usage();
        [1, usage.values().get(), usage.bytes().get()]
    }
}

// SAFETY: `Failure` fixes data at zero and stores its exact usage components.
unsafe impl<A: Amount> mem::raw::Amount<3, usize> for Failure<A> {
    fn get(self) -> [usize; 3] {
        let usage = self.0.usage();
        [0, usage.values().get(), usage.bytes().get()]
    }
}

// SAFETY: each variant preserves the exact payload request that constructed it.
unsafe impl mem::raw::Amount<3, usize> for Requested {
    fn get(self) -> [usize; 3] {
        match self {
            Self::Value(value) => mem::raw::Amount::get(value),
            Self::Failure(failure) => mem::raw::Amount::get(failure),
        }
    }
}

// SAFETY: each variant stores the exact usage and one statically nonzero axis.
unsafe impl mem::raw::Amount<3, usize> for Used {
    fn get(self) -> [usize; 3] {
        match self {
            Self::Values { values, bytes } => [0, values.0.get(), bytes.get()],
            Self::Bytes(bytes) => [0, 0, bytes.0.get()],
        }
    }
}

impl Values {
    pub(in crate::reply::retain) const fn new(value: num::NonZero<usize>) -> Self {
        Self(value)
    }
}

impl Bytes {
    pub(in crate::reply::retain) const fn new(value: num::NonZero<usize>) -> Self {
        Self(value)
    }
}

impl<A: Amount> Value<A> {
    pub(in crate::reply) const fn new(amount: A) -> Self {
        Self(amount)
    }

    pub(in crate::reply) fn usage(self) -> usage::Usage {
        self.0.usage()
    }

    pub(in crate::reply) fn split(self) -> (Data, Failure<A>) {
        (Data, Failure(self.0))
    }
}

impl Failure {
    pub(in crate::reply) const fn new(usage: usage::Usage) -> Self {
        Self(usage)
    }

    pub(in crate::reply) const fn empty() -> Self {
        Self(usage::Usage::ZERO)
    }

    pub(in crate::reply) const fn usage(self) -> usage::Usage {
        self.0
    }

    pub(in crate::reply::retain) fn claim(self) -> Option<Used> {
        if let Some(values) = num::NonZero::<usize>::new(self.0.values().get()) {
            Some(Used::Values {
                values: Values(values),
                bytes: self.0.bytes(),
            })
        } else {
            Some(Used::Bytes(Bytes(num::NonZero::<usize>::new(
                self.0.bytes().get(),
            )?)))
        }
    }

    pub(in crate::reply::retain) fn split(self) -> Class {
        let values = num::NonZero::<usize>::new(self.0.values().get());
        let bytes = num::NonZero::<usize>::new(self.0.bytes().get());
        match (values, bytes) {
            (None, None) => Class::Zero,
            (Some(values), None) => Class::Values(Values(values)),
            (None, Some(bytes)) => Class::Bytes(Bytes(bytes)),
            (Some(values), Some(bytes)) => Class::Both(Values(values), Bytes(bytes)),
        }
    }
}

const _: () = {
    assert!(size_of::<Data>() == 0);
    assert!(size_of::<Value>() == 2 * size_of::<usize>());
    assert!(size_of::<Failure>() == 2 * size_of::<usize>());
    assert!(size_of::<Requested>() == 3 * size_of::<usize>());
    assert!(size_of::<Used>() == size_of::<usage::Usage>());
    assert!(size_of::<Class>() == 3 * size_of::<usize>());
    assert!(size_of::<Option<Values>>() == size_of::<usize>());
    assert!(size_of::<Option<Bytes>>() == size_of::<usize>());
};
