use std::{mem, num};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    ParentStale,
    WakeHopCeiling {
        minimum: usize,
        maximum: usize,
    },
    Capacity {
        requested: num::NonZero<usize>,
        available: usize,
    },
}

const _: () = assert!(mem::size_of::<Error>() == 3 * mem::size_of::<usize>());
