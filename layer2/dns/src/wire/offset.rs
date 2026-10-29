#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(in crate::wire) struct Offset(u16);

impl Offset {
    pub(in crate::wire) const ZERO: Self = Self(0);

    pub(in crate::wire) const fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub(in crate::wire) const fn raw(self) -> u16 {
        self.0
    }

    pub(in crate::wire) const fn get(self) -> usize {
        self.0 as usize
    }
}
