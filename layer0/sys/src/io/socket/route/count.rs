use std::{mem, num};

const LIMIT: u16 = u8::MAX as u16 + 1;

/// A nonempty dense byte-routing domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Count(num::NonZero<u16>);

/// A dense routing index issued by an admitted datagram count.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Index(u8);

/// The rejected raw routing-domain width.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Error {
    actual: u16,
}

impl Error {
    pub const fn actual(self) -> u16 {
        self.actual
    }
}

impl Count {
    pub const MIN: Self = Self(num::NonZero::<u16>::MIN);
    /// Converts an external route byte into an index admitted by this domain.
    pub const fn index(self, raw: u8) -> Option<Index> {
        if (raw as u16) < self.encode() {
            Some(Index(raw))
        } else {
            None
        }
    }

    pub const fn encode(self) -> u16 {
        self.0.get()
    }

    /// Iterates every index proved to belong to this dense domain.
    pub fn indices(self) -> impl ExactSizeIterator<Item = Index> + DoubleEndedIterator {
        (0..=u8::MAX).map(Index).take(usize::from(self.encode()))
    }
}

impl Index {
    /// Returns the dense routing index at an indexing leaf.
    pub const fn into_raw(self) -> usize {
        self.0 as usize
    }
}

impl TryFrom<u16> for Count {
    type Error = Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value > LIMIT {
            return Err(Error { actual: value });
        }
        match num::NonZero::<u16>::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(Error { actual: value }),
        }
    }
}

const _: () = assert!(mem::size_of::<Result<Count, Error>>() == 2 * mem::size_of::<u16>());
