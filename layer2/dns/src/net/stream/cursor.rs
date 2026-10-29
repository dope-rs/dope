use crate::wire::tx::Id;

/// A connection-owned, non-wrapping permutation cursor over the complete DNS
/// transaction-ID space. Request rollback never rewinds it.
#[derive(Clone, Copy)]
pub(super) struct Cursor<const N: u32> {
    start: Id,
    issued: u32,
}

impl<const N: u32> Cursor<N> {
    pub(super) const fn new(start: Id) -> Self {
        const { assert!(N != 0 && N <= u16::MAX as u32 + 1) }
        Self { start, issued: 0 }
    }

    pub(super) const fn next(self) -> Option<(Id, Self)> {
        if self.issued >= N {
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
