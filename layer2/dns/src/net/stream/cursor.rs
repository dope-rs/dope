use crate::wire::tx::Id;

/// A connection-owned, non-wrapping permutation cursor over the complete DNS
/// transaction-ID space. Request rollback never rewinds it.
#[derive(Clone, Copy)]
pub(super) struct Cursor {
    start: Id,
    issued: u32,
}

impl Cursor {
    pub(super) const fn new(start: Id) -> Self {
        Self { start, issued: 0 }
    }

    pub(super) const fn next(self) -> Option<(Id, Self)> {
        if self.issued >= Id::CAPACITY as u32 {
            return None;
        }
        let id = self.start.wrapping_add(self.issued as u16);
        Some((
            id,
            Self {
                start: self.start,
                issued: self.issued + 1,
            },
        ))
    }
}
