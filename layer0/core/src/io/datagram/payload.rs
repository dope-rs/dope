use std::{mem, num};

use crate::io::{socket, transfer};

/// Failure to represent a requested datagram payload in one backend slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty {
        actual: u32,
        minimum: u32,
    },
    TooLarge {
        payload: u32,
        prefix: u32,
        actual: u64,
        maximum: u32,
    },
}

/// Slot length that guarantees one payload byte on every backend.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Len(num::NonZero<u32>);

impl Len {
    /// Common ceiling: io_uring UAPI fields plus backend socket-address storage.
    const PREFIX: u32 = (4 * mem::size_of::<u32>() + socket::Addr::STORAGE_CAPACITY) as u32;

    pub const fn fixed<const BYTES: u32>() -> Self {
        const {
            assert!(BYTES != 0);
            assert!((Self::PREFIX as u64 + BYTES as u64) <= transfer::MAX_BYTES as u64);
        }
        Self(num::NonZero::<u32>::MIN.saturating_add(Self::PREFIX + BYTES - 1))
    }

    pub(crate) const fn nonzero(self) -> num::NonZero<u32> {
        self.0
    }
}

impl From<num::NonZero<u16>> for Len {
    fn from(payload: num::NonZero<u16>) -> Self {
        const { assert!(Len::PREFIX + u16::MAX as u32 <= transfer::MAX_BYTES as u32) };
        Self(num::NonZero::<u32>::from(payload).saturating_add(Self::PREFIX))
    }
}

impl TryFrom<u32> for Len {
    type Error = Error;

    fn try_from(payload: u32) -> Result<Self, Self::Error> {
        if payload == 0 {
            return Err(Error::Empty {
                actual: payload,
                minimum: 1,
            });
        }
        let actual = Self::PREFIX as u64 + payload as u64;
        if actual > transfer::MAX_BYTES as u64 {
            return Err(Error::TooLarge {
                payload,
                prefix: Self::PREFIX,
                actual,
                maximum: transfer::MAX_BYTES as u32,
            });
        }
        Ok(Self(
            num::NonZero::<u32>::MIN.saturating_add(actual as u32 - 1),
        ))
    }
}

const _: () = assert!(mem::size_of::<Len>() == mem::size_of::<u32>());
