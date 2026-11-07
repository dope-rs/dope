//! Reply payload ownership.

use std::{mem::size_of, num};

use o3::mem;

use crate::reply::{retain, usage, usage::amount::Amount};

pub(in crate::reply) mod raw;

pub(in crate::reply) enum Tag {}

pub(in crate::reply) type Grant<'credits, A> =
    mem::raw::Grant<retain::raw::Lane<'credits, 3, Tag>, 3, A>;

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

pub(in crate::reply::retain) enum Class<'credits> {
    Zero,
    Values(Grant<'credits, Values>),
    Bytes(Grant<'credits, Bytes>),
    Both(Grant<'credits, Values>, Grant<'credits, Bytes>),
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

struct SplitValue;
struct Reclassify<A>(A);
struct SplitFailure(Values, Bytes);

// SAFETY: Value always partitions into exactly one Data and its stored usage.
unsafe impl<A: Amount> mem::raw::Exact<3, usize, Value<A>> for SplitValue {
    type Input = ();
    type Left = Data;
    type Right = Failure<A>;
    fn parts(self, source: Value<A>, (): ()) -> (Data, Failure<A>) {
        (Data, Failure(source.0))
    }
}

// SAFETY: classification binds the exact unchanged source before reclassification.
unsafe impl<A: mem::raw::Amount<3, usize>> mem::raw::ExactRebind<3, usize, Failure>
    for Reclassify<A>
{
    type Output = A;
    fn rebind(self, _: Failure) -> A {
        self.0
    }
}

// SAFETY: classification extracts these two exact nonzero components from its source.
unsafe impl mem::raw::Exact<3, usize, Failure> for SplitFailure {
    type Input = ();
    type Left = Values;
    type Right = Bytes;
    fn parts(self, _: Failure, (): ()) -> (Values, Bytes) {
        (self.0, self.1)
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

    pub(in crate::reply) fn split<'credits>(
        grant: Grant<'credits, Self>,
    ) -> (Grant<'credits, Data>, Grant<'credits, Failure<A>>) {
        // SAFETY: SplitValue is exact for every Value, including zero usage.
        unsafe { grant.bind(SplitValue) }.split(())
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

    /// An empty grant owns no capacity and requires no refund.
    pub(in crate::reply::retain) fn claim<'credits>(
        grant: Grant<'credits, Self>,
    ) -> Option<Grant<'credits, Used>> {
        let amount = grant.amount().0;
        let used = if let Some(values) = num::NonZero::<usize>::new(amount.values().get()) {
            Used::Values {
                values: Values(values),
                bytes: amount.bytes(),
            }
        } else {
            Used::Bytes(Bytes(num::NonZero::<usize>::new(amount.bytes().get())?))
        };
        // SAFETY: classification preserves the grant's exact usage components.
        Some(unsafe { grant.bind(Reclassify(used)) }.map())
    }

    pub(in crate::reply::retain) fn split<'credits>(
        grant: Grant<'credits, Self>,
    ) -> Class<'credits> {
        let amount = grant.amount().0;
        let values = num::NonZero::<usize>::new(amount.values().get());
        let bytes = num::NonZero::<usize>::new(amount.bytes().get());
        match (values, bytes) {
            (None, None) => Class::Zero,
            (Some(values), None) => {
                // SAFETY: values is the exact sole component of the retained source.
                Class::Values(unsafe { grant.bind(Reclassify(Values(values))) }.map())
            }
            (None, Some(bytes)) => {
                // SAFETY: bytes is the exact sole component of the retained source.
                Class::Bytes(unsafe { grant.bind(Reclassify(Bytes(bytes))) }.map())
            }
            (Some(values), Some(bytes)) => {
                // SAFETY: both exact nonzero source components were extracted above.
                let (values, bytes) =
                    unsafe { grant.bind(SplitFailure(Values(values), Bytes(bytes))) }.split(());
                Class::Both(values, bytes)
            }
        }
    }
}

const _: () = {
    assert!(size_of::<Grant<'_, Data>>() == size_of::<usize>());
    assert!(size_of::<Grant<'_, Value>>() == 3 * size_of::<usize>());
    assert!(size_of::<Grant<'_, Failure>>() == 3 * size_of::<usize>());
    assert!(size_of::<Requested>() == 3 * size_of::<usize>());
    assert!(size_of::<Used>() == size_of::<usage::Usage>());
    assert!(size_of::<Grant<'_, Used>>() == 3 * size_of::<usize>());
    assert!(size_of::<Class<'_>>() == 5 * size_of::<usize>());
    assert!(size_of::<Option<Grant<'_, Values>>>() == 2 * size_of::<usize>());
    assert!(size_of::<Option<Grant<'_, Bytes>>>() == 2 * size_of::<usize>());
};
