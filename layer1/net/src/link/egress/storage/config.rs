use std::num::NonZero;

use dope_core::io::transfer;
use o3::depot::capacity;

use crate::link::egress::{Credit, Entries, Fit, Resident, metadata};

const CAP_BYTES: u32 = 1 << 20;
const CAP_ENTRIES: u32 = 1 << 12;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    kind: Kind,
    shared: Credit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Split,
    Shared,
}

impl Config {
    pub const DEFAULT: Self = Self {
        kind: Kind::Split,
        shared: Credit::new(Entries::new(CAP_ENTRIES / 2), Resident::new(CAP_BYTES / 2)),
    };

    pub const fn shared(shared: Credit) -> Self {
        Self {
            kind: Kind::Shared,
            shared,
        }
    }

    pub(in crate::link::egress) const fn shared_limit(self) -> [u32; 2] {
        [self.shared.entries().raw(), self.shared.resident().raw()]
    }

    const fn total(self) -> Credit {
        match self.kind {
            Kind::Split => Credit::new(Entries::new(CAP_ENTRIES), Resident::new(CAP_BYTES)),
            Kind::Shared => self.shared,
        }
    }

    pub(in crate::link::egress) const fn reserved(self) -> Credit {
        match self.kind {
            Kind::Split => Self::DEFAULT.shared,
            Kind::Shared => Credit::new(Entries::new(0), Resident::ZERO),
        }
    }

    /// Maximum retained metadata entries owned by one arena.
    pub const fn entries(self) -> Entries {
        self.total().entries()
    }

    /// Maximum resident payload bytes owned by one arena.
    pub const fn bytes(self) -> Resident {
        self.total().resident()
    }

    /// Maximum non-empty fixed-size chunks admitted by both credit axes.
    pub const fn chunks(self, bytes: transfer::NonZero) -> Option<NonZero<u32>> {
        let bytes = bytes.into_nonzero().get();
        let entries = self.entries().raw();
        let resident = self.bytes().raw() / bytes;
        let chunks = if entries < resident {
            entries
        } else {
            resident
        };
        capacity::Count::new(chunks).nonzero()
    }

    pub(in crate::link::egress) fn flights(self, lanes: metadata::LaneCount) -> capacity::Count {
        capacity::Count::new(lanes.position().get().min(self.entries().raw()))
    }

    pub(in crate::link::egress) fn fits_all(self, lanes: metadata::LaneCount, req: Credit) -> Fit {
        let avail = self.shared.capped_add(self.reserved().floor(lanes));
        if req.entries() > avail.entries() {
            Fit::Entries
        } else if req.resident() > avail.resident() {
            Fit::Resident
        } else {
            Fit::Admitted
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::DEFAULT
    }
}

const _: () = assert!(size_of::<Config>() == 3 * size_of::<u32>());
