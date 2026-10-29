use dope_core::io::transfer;

use crate::link::egress::{Geometry, Resident, ResidentSize, WireBytes, data, load};

/// Exact final-link accounting for one retained payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Charge {
    wire: WireBytes,
    resident: ResidentSize,
}

impl Charge {
    pub fn of<'d, B: data::raw::Payload<'d>>(value: &B) -> Self {
        let bytes = value.len();
        let owner = value.charge();
        Self {
            wire: WireBytes::new(bytes),
            resident: ResidentSize::new(owner.resident_bytes),
        }
    }

    pub const fn bytes(self) -> WireBytes {
        self.wire
    }

    pub const fn resident(self) -> ResidentSize {
        self.resident
    }

    pub(in crate::link::egress) const fn new(wire: WireBytes, resident: ResidentSize) -> Self {
        Self { wire, resident }
    }

    /// Validates this measured charge as one non-empty queue geometry.
    pub fn try_geometry(self) -> Result<Geometry, load::Error> {
        let wire = transfer::Len::try_new(self.wire.get()).map_err(load::Error::Transfer)?;
        let resident = Resident::try_from(self.resident.get()).map_err(load::Error::Resident)?;
        let wire = transfer::NonZero::try_from(wire).map_err(load::Error::Empty)?;
        Ok(Geometry::new(wire, resident))
    }
}

const _: () = assert!(size_of::<Charge>() == 2 * size_of::<usize>());
