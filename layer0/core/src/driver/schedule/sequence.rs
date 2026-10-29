use std::{cell, hash, mem, num};

use o3::base::marker::ThreadBound;

use crate::driver;

const LOCAL: u64 = u32::MAX as u64;
const BLOCK: u64 = 1_u64 << u32::BITS;

pub(in crate::driver) struct Owner(cell::Cell<Option<num::NonZero<u64>>>);

impl Owner {
    pub(in crate::driver) const fn new() -> Self {
        Self(cell::Cell::new(Some(num::NonZero::<u64>::MIN)))
    }

    fn claim(&self) -> Option<num::NonZero<u64>> {
        let first = self.0.get()?;
        let skip = 1_u32.wrapping_sub(first.get() as u32);
        let first = first.checked_add(u64::from(skip))?;
        self.0.set(first.checked_add(BLOCK));
        Some(first)
    }

    pub(in crate::driver) fn next(&self) -> Option<num::NonZero<u64>> {
        let next = self.0.get()?;
        self.0.set(next.checked_add(1));
        Some(next)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Exhausted;

/// A sequence identity issued by one live driver.
///
/// The invariant lifetime brand prevents identities from independent driver
/// instances from being retained or compared after their issuing driver ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Id<'d> {
    raw: num::NonZero<u64>,
    driver: driver::Invariant<'d>,
    thread: ThreadBound,
}

impl<'d> Id<'d> {
    pub(in crate::driver) fn new(driver: driver::Reference<'d>, raw: num::NonZero<u64>) -> Self {
        Self {
            raw,
            driver: driver.brand,
            thread: ThreadBound::NEW,
        }
    }
}

impl hash::Hash for Id<'_> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

pub struct Block<'d> {
    driver: driver::Reference<'d>,
    next: cell::Cell<Option<num::NonZero<u64>>>,
}

impl<'d> Block<'d> {
    pub(in crate::driver) fn new(driver: driver::Reference<'d>) -> Self {
        let next = driver.shared.context().sequence.claim();
        Self {
            driver,
            next: cell::Cell::new(next),
        }
    }

    pub fn try_next(&self) -> Result<Id<'d>, Exhausted> {
        let raw = match self.next.get() {
            Some(raw) => raw,
            None => self
                .driver
                .shared
                .context()
                .sequence
                .claim()
                .ok_or(Exhausted)?,
        };
        let next = raw.checked_add(1).filter(|next| next.get() & LOCAL != 0);
        self.next.set(next);
        Ok(Id::new(self.driver, raw))
    }
}

const _: () = {
    assert!(mem::size_of::<Block<'static>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Id<'static>>() == mem::size_of::<num::NonZero<u64>>());
    assert!(mem::size_of::<Result<Id<'static>, Exhausted>>() == mem::size_of::<usize>());
};
