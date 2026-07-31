use super::super::WireLease;

pub(in crate::link::egress) struct WirePointer(*const u8);

impl WirePointer {
    pub(in crate::link::egress) fn at(buffer: &WireLease<'_>, start: usize) -> Self {
        debug_assert!(start <= buffer.len());
        Self(buffer.as_ref()[start..].as_ptr())
    }

    pub(in crate::link::egress) fn get(self) -> *const u8 {
        self.0
    }
}
