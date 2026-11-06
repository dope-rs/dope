use std::{marker, mem, num, pin};

use o3::{alloc, depot::capacity};

mod branch;
mod leaf;
mod proof;

pub use branch::Branch;
pub use leaf::Leaf;
pub(crate) use proof::Proof;

/// Number of task slots represented by one fixed scheduler tree.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Tasks(num::NonZero<u32>);

impl Tasks {
    pub const fn nonzero(self) -> num::NonZero<u32> {
        self.0
    }

    pub(crate) const fn add(self, other: Self) -> Self {
        assert!(other.0.get() <= u32::MAX - self.0.get());
        Self(self.0.saturating_add(other.0.get()))
    }

    pub(crate) const fn from(capacity: Capacity) -> Self {
        Self(capacity.0)
    }
}

/// Number of slots owned by one fixed scheduler member.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Capacity(num::NonZero<u32>);

impl Capacity {
    pub(crate) const fn new(value: num::NonZero<u32>) -> Self {
        Self(value)
    }

    pub const fn nonzero(self) -> num::NonZero<u32> {
        self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Tasks>() == mem::size_of::<num::NonZero<u32>>());
    assert!(mem::size_of::<Capacity>() == mem::size_of::<num::NonZero<u32>>());
};

pub trait Member<'d> {
    type Fiber;
    type Domain;
    type Target: Copy;

    const CAPACITY: Capacity;
}

pub struct Here;

pub struct Left<P>(marker::PhantomData<fn() -> P>);

pub struct Right<P>(marker::PhantomData<fn() -> P>);

/// A structurally defined task tree.
///
/// Custom projections cannot substitute a different pool behind a selection.
///
pub trait Tree<'d>: Proof {
    type First;

    const TASKS: Tasks;
}

/// Constructs each route's storage before the task tree is pinned or published.
pub trait Build<'d>: Tree<'d> + Sized {
    fn try_build() -> Result<Self, alloc::Error> {
        Self::try_build_with_capacity(capacity::Count::MAX)
    }

    /// Allocates at most `physical` slots per route, preserving declared indices.
    fn try_build_with_capacity(physical: capacity::Count) -> Result<Self, alloc::Error>;
}

pub trait At<'d, P>: Tree<'d> + Proof<P> {
    type Member: Member<'d>;

    /// Projects the pinned tree to the selected member.
    fn project(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Member>;

    /// Borrows the selected member.
    fn project_ref(&self) -> &Self::Member;
}
