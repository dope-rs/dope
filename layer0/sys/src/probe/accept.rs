use o3::base::num::bounded;

use crate::{
    memory,
    probe::{Cause, Outcome, Stopped, cleanup::Rejection},
    ring,
    ring::register::Occupied,
};

/// Exact position rejected before or after the fixed-file ABI check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outside {
    Abi(bounded::U32<0, { i32::MAX as u32 }>),
    Capacity {
        actual: bounded::U32<0, { (1 << 24) - 1 }>,
        limit: bounded::U32<0, { (1 << 24) - 1 }>,
    },
}

/// Exact accept completion evidence outside the admitted contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Check<P> {
    Early(P),
    Outside { probe: P, cause: Outside },
}

/// Runs direct and multishot accept canaries on an owned ring.
pub trait Validate<L: memory::raw::Canary, P: Copy, R: Copy>: Sized {
    type Range: Range;
    type Probe: Copy;
    type Update;
    type Cleanup;
    type Release;

    fn accept(
        self,
        range: Self::Range,
        release: impl FnOnce(Self::Probe, Self::Update) -> R,
    ) -> Result<Self, Rejection<Self::Cleanup, Self::Release>>;
}

/// A successfully admitted range and its reserved listener slot.
pub trait Range {
    type Target;
    type Value;

    fn fixed(&self) -> Self::Target;
    fn accepted(&self, observed: Self::Value) -> Result<Self::Target, Outside>;
}

type Fault<A> = Cause<
    <A as Accept>::Probe,
    <<A as Accept>::Outcome as Outcome>::Observation,
    <A as Accept>::Error,
>;

/// An active accept canary owns its ring until cancellation or descriptor close.
pub trait Accept: Sized {
    type Ring: ring::Owner<File = Self::File, Closing = Self::Closing>;
    type Owned;
    type Probe: Copy;
    type Outcome: Outcome<Probe = Self::Probe>;
    type Error;
    type File: Occupied;
    type Closing;

    fn new(ring: Self::Ring, probe: Self::Probe) -> Self;
    fn submit(
        self,
        entry: Self::Owned,
    ) -> Result<(Self, Self::Outcome), Stopped<Self, Fault<Self>>>;
    fn cancel(self) -> Result<Self::Ring, Stopped<Self, Fault<Self>>>;
    fn try_close(self, file: Self::File) -> Result<(), Self::Closing>;
}
