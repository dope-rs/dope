use crate::driver::route::indices;
mod capacity;
mod overflow;
mod scope;

use std::{marker, mem, num};

pub use capacity::Capacity;
pub use overflow::Overflow;
pub use scope::Scope;

use crate::driver::route;

/// Exact lane selected by an external task-owner identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Lane(num::NonZero<u32>);

/// The only `u32` lane without a nonzero stored representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Invalid;

impl Lane {
    /// Nonzero position retained by this admitted lane.
    pub const fn position(self) -> num::NonZero<u32> {
        self.0
    }

    /// Mechanical index used only when selecting external backing storage.
    pub const fn index(self) -> usize {
        self.0.get() as usize - 1
    }
}

impl TryFrom<u32> for Lane {
    type Error = Invalid;

    fn try_from(raw: u32) -> Result<Self, Self::Error> {
        match raw.checked_add(1).and_then(num::NonZero::<u32>::new) {
            Some(stored) => Ok(Self(stored)),
            None => Err(Invalid),
        }
    }
}

impl From<indices::SlotIndex> for Lane {
    fn from(slot: indices::SlotIndex) -> Self {
        Self(slot.stored())
    }
}

impl From<num::NonZero<u32>> for Lane {
    fn from(position: num::NonZero<u32>) -> Self {
        Self(position)
    }
}

/// Nonzero stored form of one complete owner generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Generation(num::NonZero<u64>);

/// Exact rejected owner coordinates, independent of the issuing source type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Foreign {
        lane: Lane,
        generation: Generation,
    },
    Stale {
        lane: Lane,
        generation: Generation,
        current: Option<Generation>,
    },
}

impl Generation {
    pub const fn nonzero(self) -> num::NonZero<u64> {
        self.0
    }

    pub const fn from_u32(raw: u32) -> Self {
        Self(num::NonZero::<u64>::MIN.saturating_add(raw as u64))
    }

    pub const fn from_route(epoch: route::Epoch) -> Self {
        Self(epoch.get())
    }
}

/// A complete external identity that can be projected into one owner lane.
pub trait Identity {
    fn parts(self) -> (Lane, Generation);
}

/// Selects the one external identity allowed to issue owners for a source.
pub trait Source {
    type Identity: Identity;
}

/// Exact reusable lane and generation issued by one statically tagged owner source.
#[derive(Eq)]
pub struct Owner<S = ()> {
    lane: Lane,
    generation: Generation,
    source: marker::PhantomData<fn(S) -> S>,
}

impl<S> Clone for Owner<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Owner<S> {}

impl<S> Owner<S> {
    pub fn from(identity: S::Identity) -> Self
    where
        S: Source,
    {
        let (lane, stamp) = identity.parts();
        Self {
            lane,
            generation: stamp,
            source: marker::PhantomData,
        }
    }

    pub const fn lane(self) -> Lane {
        self.lane
    }

    pub const fn generation(self) -> Generation {
        self.generation
    }
}

impl<S> PartialEq for Owner<S> {
    fn eq(&self, other: &Self) -> bool {
        self.lane == other.lane && self.generation == other.generation
    }
}

const _: () = {
    assert!(mem::size_of::<Lane>() == mem::size_of::<u32>());
    assert!(mem::align_of::<Lane>() == mem::align_of::<u32>());
    assert!(mem::size_of::<Option<Lane>>() == mem::size_of::<Lane>());
    assert!(mem::size_of::<Result<Lane, Invalid>>() == mem::size_of::<Lane>());
    assert!(mem::size_of::<Generation>() == mem::size_of::<num::NonZero<u64>>());
    assert!(mem::size_of::<Option<Generation>>() == mem::size_of::<Generation>());
    assert!(mem::size_of::<Owner>() == mem::size_of::<(Lane, Generation)>());
    assert!(mem::size_of::<Option<Owner>>() == mem::size_of::<Owner>());
};
