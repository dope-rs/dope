use std::num;

use crate::{
    driver::settings::{Buffers, demands, errors},
    io::{datagram::payload, transfer},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Receive {
    pub(in crate::driver::settings) entries: Buffers,
    pub(in crate::driver::settings) len: transfer::NonZero,
}

impl Receive {
    pub const DEFAULT: Self = Self::fixed::<4096, 4096>();
    pub const MIN_DATAGRAM_SLOT: transfer::NonZero =
        transfer::NonZero::from_payload(payload::Len::fixed::<1>());

    pub const fn fixed<const ENTRIES: u16, const LEN: u32>() -> Self {
        const {
            assert!(LEN != 0);
            assert!(LEN <= transfer::MAX_BYTES as u32);
            assert!((ENTRIES as usize) <= (isize::MAX as usize) / (LEN as usize));
            Self {
                entries: Buffers::fixed::<ENTRIES>(),
                len: transfer::NonZero::fixed::<LEN>(),
            }
        }
    }

    pub(super) const fn from_parts(entries: Buffers, len: transfer::NonZero) -> Self {
        Self { entries, len }
    }

    pub fn resolve(self, need: demands::Receive) -> Result<Self, errors::Claim> {
        let base = demands::Receive::from(self);
        let entries = if base.entries().get() >= need.entries().get() {
            base.entries()
        } else {
            need.entries()
        };
        let len = base.buffer_len().max(need.buffer_len());
        Self::exact(demands::Receive::new(entries, len))
    }

    pub fn exact(need: demands::Receive) -> Result<Self, errors::Claim> {
        let entries = Buffers::try_from(need.entries().get())?;
        let len = transfer::NonZero::try_from(need.buffer_len())
            .map_err(errors::Config::ReceiveBuffer)?;
        Ok(Self::from_parts(entries, len))
    }

    pub const fn buffers(self) -> Buffers {
        self.entries
    }
    pub(crate) const fn entries(self) -> u16 {
        self.entries.get()
    }
    pub(crate) const fn count(self) -> num::NonZero<u16> {
        self.entries.0
    }
    /// Keeps one receive slot available while other buffers remain accounted.
    pub(crate) const fn accounts(self) -> u32 {
        self.entries.get() as u32 - 1
    }
    /// Returns the positive slot length proven representable by every backend.
    pub const fn buffer_len(self) -> transfer::NonZero {
        self.len
    }
}
