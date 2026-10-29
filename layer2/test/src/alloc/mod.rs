//! Thread-local observations of runtime transactions and injected allocation failure.

use std::{array, cell::Cell};

use o3::base::marker::ThreadBound;

mod raw;

thread_local! {
    static COUNTS: Cell<[usize; 4]> = const { Cell::new([0; 4]) };
    static REMAINING: Cell<Option<usize>> = const { Cell::new(None) };
}

#[derive(Debug, Default, PartialEq)]
pub struct Activity {
    pub allocations: usize,
    pub allocated_bytes: usize,
    pub deallocations: usize,
    pub deallocated_bytes: usize,
}

impl From<[usize; 4]> for Activity {
    fn from([made, bytes, freed, released]: [usize; 4]) -> Self {
        Self {
            allocations: made,
            allocated_bytes: bytes,
            deallocations: freed,
            deallocated_bytes: released,
        }
    }
}

/// Keeps a transaction's measurement on its original thread across call scopes.
pub struct Measurement {
    before: ([usize; 4], ThreadBound),
}

impl Measurement {
    pub fn finish(self) -> Activity {
        difference(self.before.0).into()
    }
}

/// System allocation with observation confined to the calling test thread.
#[derive(Default)]
pub struct Tracker;

impl Tracker {
    pub const fn new() -> Self {
        Self
    }

    pub fn begin() -> Measurement {
        Measurement {
            before: (COUNTS.with(Cell::get), ThreadBound::NEW),
        }
    }

    pub fn measure<T>(run: impl FnOnce() -> T) -> (T, [usize; 4]) {
        let before = COUNTS.with(Cell::get);
        let output = run();
        (output, difference(before))
    }

    pub fn activity<T>(run: impl FnOnce() -> T) -> (T, Activity) {
        let (output, counts) = Self::measure(run);
        (output, counts.into())
    }

    pub fn usage<T>(run: impl FnOnce() -> T) -> (T, (usize, usize)) {
        let (output, counts) = Self::measure(run);
        (output, (counts[0], counts[1]))
    }

    pub fn count<T>(run: impl FnOnce() -> T) -> (T, usize) {
        let (output, counts) = Self::measure(run);
        (output, counts[0])
    }

    pub fn counts<T>(run: impl FnOnce() -> T) -> (T, [usize; 2]) {
        let (output, counts) = Self::measure(run);
        (output, [counts[0], counts[2]])
    }

    pub fn during(run: impl FnOnce()) -> (usize, usize) {
        Self::usage(run).1
    }

    /// Rejects one allocation attempt, then permits cleanup and subsequent work.
    pub fn fail_after<T>(allows: usize, run: impl FnOnce() -> T) -> T {
        let prior = REMAINING.with(|left| left.replace(Some(allows)));
        let output = run();
        REMAINING.with(|left| left.set(prior));
        output
    }
}

fn difference(before: [usize; 4]) -> [usize; 4] {
    let after = COUNTS.with(Cell::get);
    array::from_fn(|index| after[index].wrapping_sub(before[index]))
}
