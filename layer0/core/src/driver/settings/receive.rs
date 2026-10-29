use std::num;

use crate::{
    driver::settings::{Buffers, Ring, demands, errors},
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
            assert!(ENTRIES >= 2);
            assert!(ENTRIES.is_power_of_two());
            assert!(LEN != 0);
            assert!(LEN <= transfer::MAX_BYTES as u32);
            assert!((ENTRIES as usize) <= (isize::MAX as usize) / (LEN as usize));
            Self {
                entries: Buffers(num::NonZero::<u16>::MIN.saturating_add(ENTRIES - 1)),
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
        let entries = Ring::try_round(need.entries().get())?;
        let len = transfer::NonZero::try_from(need.buffer_len())
            .map_err(errors::Config::ReceiveBuffer)?;
        Self::assemble(entries, len).map_err(Into::into)
    }

    pub(super) const fn assemble(
        entries: Ring,
        len: transfer::NonZero,
    ) -> Result<Self, errors::Config> {
        let entries = entries.buffers();
        let raw = len.into_nonzero().get();
        let bytes = entries.get() as u64 * raw as u64;
        if bytes > isize::MAX as u64 {
            return Err(errors::Config::ReceiveAllocationTooLarge {
                entries: entries.get(),
                buffer_len: raw,
                actual: bytes,
                maximum: isize::MAX as u64,
            });
        }
        Ok(Self { entries, len })
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
