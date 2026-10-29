use std::{marker, mem, num, pin};

use o3::alloc;

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

    pub(crate) const fn contains(self, offset: Offset, capacity: Capacity) -> bool {
        let tasks = self.0.get();
        offset.0 <= tasks && capacity.0.get() <= tasks - offset.0
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

    pub(crate) const fn is_same(self, other: Self) -> bool {
        self.0.get() == other.0.get()
    }
}

/// Offset of one selected member in its fixed scheduler tree.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Offset(u32);

impl Offset {
    pub(crate) const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn into_u32(self) -> u32 {
        self.0
    }

    pub(crate) const fn shift(self, tasks: Tasks) -> Self {
        assert!(self.0 <= u32::MAX - tasks.0.get());
        Self(self.0 + tasks.0.get())
    }
}

const _: () = {
    assert!(mem::size_of::<Tasks>() == mem::size_of::<num::NonZero<u32>>());
    assert!(mem::size_of::<Capacity>() == mem::size_of::<num::NonZero<u32>>());
    assert!(mem::size_of::<Offset>() == mem::size_of::<u32>());
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
    const TASKS: Tasks;
}

/// Constructs each route's storage before the task tree is pinned or published.
pub trait Build<'d>: Tree<'d> + Sized {
    fn try_build() -> Result<Self, alloc::Error>;
}

pub trait At<'d, P>: Tree<'d> + Proof<P> {
    type Member: Member<'d>;

    const OFFSET: Offset;

    /// Projects the pinned tree to the selected member.
    fn project(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::Member>;

    /// Borrows the selected member.
    fn project_ref(&self) -> &Self::Member;
}
