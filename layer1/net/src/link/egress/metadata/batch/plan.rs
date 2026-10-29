use crate::link::egress::{self, load::credit, metadata::batch};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct Plan(credit::Amount);

impl Plan {
    pub const fn new(
        entries: egress::Entries,
        resident: egress::Resident,
    ) -> Result<Self, batch::Empty> {
        if entries.is_empty() {
            return Err(batch::Empty);
        }
        Ok(Self(credit::Amount::new(entries, resident)))
    }

    pub(in crate::link::egress) const fn amount(self) -> credit::Amount {
        self.0
    }
}

const _: () = assert!(std::mem::size_of::<Plan>() == std::mem::size_of::<u64>());
