use dope_core::io::transfer;

use crate::link::egress::{
    self,
    load::{self, Value},
    reserve::{Bounds, exact, limit},
};

/// Exact reason requested final-link geometry cannot be represented or admitted.
#[derive(Debug)]
pub enum Full {
    Limit { part: Part, cause: limit::Error },
    Pair(Pair),
    Lane(Lane),
    Admission(egress::Admission),
    Reserve(exact::Failure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Part {
    One,
    First,
    Second,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mismatch {
    First(limit::Mismatch),
    Second(limit::Mismatch),
    UnexpectedSecond {
        wire: transfer::NonZero,
        resident: egress::Resident,
    },
}

/// Exact resident bounds whose sum is not representable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pair {
    cause: egress::Overflow<egress::Resident>,
}

impl Pair {
    pub(super) const fn new(cause: egress::Overflow<egress::Resident>) -> Self {
        Self { cause }
    }

    pub const fn first(self) -> egress::Resident {
        *self.cause.left()
    }

    pub const fn second(self) -> egress::Resident {
        *self.cause.right()
    }

    pub const fn maximum() -> egress::Resident {
        egress::Resident::new(u32::MAX)
    }
}

/// Exact bounds that cannot fit their owning final-link lane.
#[derive(Clone, Copy, Debug)]
pub struct Lane {
    bounds: Bounds,
}

impl Lane {
    pub(super) const fn new(bounds: Bounds) -> Self {
        Self { bounds }
    }

    pub const fn bounds(self) -> Bounds {
        self.bounds
    }
}

/// A payload recovered with its exact capacity admission failure.
pub struct Capacity<B> {
    pub load: egress::NonEmpty<B>,
    pub cause: egress::Admission,
}

impl<B> Capacity<B> {
    pub fn into_parts(self) -> (egress::NonEmpty<B>, egress::Admission) {
        (self.load, self.cause)
    }
}

/// Exact reason a multi-value publication was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Changed(Mismatch),
    Invalid { part: Part, reason: load::Error },
}

/// Values recovered before any queue-visible mutation.
pub struct Error<B> {
    cause: Cause,
    bounds: Bounds,
    first: Option<Value<B>>,
    second: Option<Value<B>>,
}

impl<B> Error<B> {
    pub(super) const fn changed(
        cause: Mismatch,
        bounds: Bounds,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
    ) -> Self {
        Self {
            cause: Cause::Changed(cause),
            bounds,
            first,
            second,
        }
    }

    pub(super) const fn invalid(
        part: Part,
        reason: load::Error,
        bounds: Bounds,
        first: Option<Value<B>>,
        second: Option<Value<B>>,
    ) -> Self {
        Self {
            cause: Cause::Invalid { part, reason },
            bounds,
            first,
            second,
        }
    }

    pub const fn cause(&self) -> Cause {
        self.cause
    }

    pub const fn bounds(&self) -> Bounds {
        self.bounds
    }

    pub fn into_parts(self) -> (Cause, Bounds, Option<Value<B>>, Option<Value<B>>) {
        (self.cause, self.bounds, self.first, self.second)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Pair>() == 2 * std::mem::size_of::<u32>());
    assert!(std::mem::size_of::<Lane>() == std::mem::size_of::<Bounds>());
};
