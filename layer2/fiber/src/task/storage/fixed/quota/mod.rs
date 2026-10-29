use std::{marker, mem};

use crate::task::storage::fixed::tree;

mod state;

pub use state::State;

#[must_use = "a quota vacancy must be committed or abandoned"]
pub struct Vacancy<Q>(marker::PhantomData<fn(Q) -> Q>);

/// Zero-sized proof that one admitted task identity was retired.
pub struct Retired(());

impl<Q> Vacancy<Q> {
    const fn new() -> Self {
        Self(marker::PhantomData)
    }
}

impl Retired {
    pub(in crate::task::storage::fixed) const fn new() -> Self {
        Self(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct Physical;

impl State for Physical {
    fn capacity(self) -> super::Capacity {
        super::Capacity::Available
    }

    fn vacancy(self) -> Option<Vacancy<Self>> {
        Some(Vacancy::new())
    }

    fn consume(self, vacancy: Vacancy<Self>) -> Self {
        let Vacancy(marker::PhantomData) = vacancy;
        self
    }

    fn retire(self, retired: Retired) -> Self {
        let Retired(()) = retired;
        self
    }
}

#[derive(Clone, Copy)]
pub struct Bounded {
    remaining: Remaining,
}

/// The configured logical admission limit for a fixed task tree.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Limit(usize);

/// Logical admissions still available in a bounded fixed task tree.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Remaining(usize);

impl Limit {
    pub const fn new(limit: usize) -> Self {
        Self(limit)
    }

    const fn get(self) -> usize {
        self.0
    }
}

impl Remaining {
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Bounded {
    pub(in crate::task::storage::fixed) const fn new(limit: Limit, physical: tree::Tasks) -> Self {
        let physical = physical.nonzero().get() as usize;
        Self {
            remaining: Remaining(if limit.get() < physical {
                limit.get()
            } else {
                physical
            }),
        }
    }

    /// Returns the number of remaining logical admissions.
    pub const fn available(&self) -> Remaining {
        self.remaining
    }
}

impl State for Bounded {
    fn capacity(self) -> super::Capacity {
        if self.remaining.get() == 0 {
            super::Capacity::Full
        } else {
            super::Capacity::Available
        }
    }

    fn vacancy(self) -> Option<Vacancy<Self>> {
        (self.remaining.get() != 0).then_some(Vacancy::new())
    }

    fn consume(self, vacancy: Vacancy<Self>) -> Self {
        let Vacancy(marker::PhantomData) = vacancy;
        Self {
            remaining: Remaining(self.remaining.get() - 1),
        }
    }

    fn retire(self, retired: Retired) -> Self {
        let Retired(()) = retired;
        Self {
            remaining: Remaining(self.remaining.get() + 1),
        }
    }
}

const _: () = assert!(mem::size_of::<Physical>() == 0);
const _: () = assert!(mem::size_of::<Bounded>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Limit>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Remaining>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Vacancy<Bounded>>() == 0);
const _: () = assert!(mem::size_of::<Retired>() == 0);
