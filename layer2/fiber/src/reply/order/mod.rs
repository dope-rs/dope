//! Protocol ordering exposed by reply arenas.

use std::{marker::PhantomData, mem};

use crate::reply::{arena::state::EntryKey, layout::Domain};

mod depth;

pub use depth::Depth;

/// Original final reply entry observed in one arena.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct Cutoff<'arena>(Option<EntryKey>, PhantomData<fn(&'arena ()) -> &'arena ()>);

impl<'arena> Cutoff<'arena> {
    pub const EMPTY: Self = Self(None, PhantomData);

    pub const fn is_empty(self) -> bool {
        self.0.is_none()
    }

    pub(in crate::reply) const fn new(end: Option<EntryKey>, _: Domain<'arena>) -> Self {
        Self(end, PhantomData)
    }

    pub(in crate::reply) const fn key(self) -> Option<EntryKey> {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Cutoff<'static>>() == mem::size_of::<u64>());

/// Whether the original reply order remains queued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum State {
    Linked,
    Detached,
}

/// The kind of item at the front of one protocol lane.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Front {
    Empty,
    Open,
    Denied,
    Detached,
}
