use dope_core::io::transfer;
use o3::buffer::{bytes::bounded::Overflow, storage};

use crate::link::egress::{Charge, Geometry, NonEmpty, Resident, ResidentOut, Shortage, load};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Transfer(Overflow),
    Resident(ResidentOut),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mismatch {
    Credit(Shortage),
    Empty {
        actual: transfer::NonZero,
    },
    Exact {
        expected: transfer::NonZero,
        actual: transfer::NonZero,
    },
    Maximum {
        maximum: transfer::NonZero,
        actual: transfer::NonZero,
    },
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Limit {
    wire: Wire,
    resident: Resident,
}

#[derive(Clone, Copy, Debug)]
#[repr(C, u8)]
enum Wire {
    Empty,
    Exact(transfer::NonZero),
    AtMost(transfer::NonZero),
    Open,
}

impl Limit {
    pub(super) const EMPTY: Self = Self {
        wire: Wire::Empty,
        resident: Resident::ZERO,
    };

    pub(super) const fn bounded<const BYTES: usize>(resident: Resident) -> Self {
        const { assert!(BYTES != 0 && BYTES <= transfer::MAX_BYTES) }
        Self {
            wire: Wire::AtMost(transfer::NonZero::fixed_size::<BYTES>()),
            resident,
        }
    }

    pub(super) const fn open(resident: Resident) -> Self {
        Self {
            wire: Wire::Open,
            resident,
        }
    }

    pub const fn at_most<const BYTES: usize, const RESIDENT: u32>() -> Self {
        Self::bounded::<BYTES>(Resident::new(RESIDENT))
    }

    pub(in crate::link::egress) const fn proven(geometry: Geometry) -> Self {
        Self {
            wire: Wire::Exact(geometry.wire()),
            resident: geometry.resident(),
        }
    }

    pub fn try_exact(charge: Charge) -> Result<Self, Error> {
        match charge.try_geometry() {
            Ok(geometry) => Ok(Self::proven(geometry)),
            Err(load::Error::Empty(storage::Zero)) => Ok(Self::EMPTY),
            Err(load::Error::Transfer(cause)) => Err(Error::Transfer(cause)),
            Err(load::Error::Resident(cause)) => Err(Error::Resident(cause)),
        }
    }

    pub(super) const fn is_active(self) -> bool {
        !matches!(self.wire, Wire::Empty)
    }

    pub(super) const fn resident(self) -> Resident {
        self.resident
    }

    pub(in crate::link::egress::reserve) fn check(
        self,
        wire: transfer::NonZero,
    ) -> Result<(), Mismatch> {
        match self.wire {
            Wire::Empty => return Err(Mismatch::Empty { actual: wire }),
            Wire::Exact(expected) => {
                if wire != expected {
                    return Err(Mismatch::Exact {
                        expected,
                        actual: wire,
                    });
                }
            }
            Wire::AtMost(maximum) => {
                if wire.into_nonzero() > maximum.into_nonzero() {
                    return Err(Mismatch::Maximum {
                        maximum,
                        actual: wire,
                    });
                }
            }
            Wire::Open => {}
        }
        Ok(())
    }
}

impl<B> From<&NonEmpty<B>> for Limit {
    fn from(load: &NonEmpty<B>) -> Self {
        Self::proven(load.geometry())
    }
}

const _: () = assert!(std::mem::size_of::<Limit>() == 3 * std::mem::size_of::<u32>());
