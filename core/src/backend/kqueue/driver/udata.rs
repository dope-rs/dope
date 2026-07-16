use crate::driver::token::{self, Token};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Udata(u64);

impl Udata {
    const EPOCH_MASK: u64 = token::EPOCH_MASK;
    const SLOT_SHIFT: u32 = token::SLOT_BITS;
    const SLOT_MASK: u64 = token::SLOT_MASK;
    const TAG_SHIFT: u32 = 48;
    const TAG_MASK: u64 = 0xFF;
    const ROUTE_SHIFT: u32 = token::ROUTE_SHIFT;
    const ROUTE_MASK: u64 = 0xFF;

    pub(super) fn slot_key(route: u8, slot: u32) -> usize {
        ((route as usize) << token::SLOT_BITS) | (slot as usize)
    }

    pub(crate) const fn pack(tag: u8, slot: u32, epoch: u32) -> Self {
        let raw = (epoch as u64 & Self::EPOCH_MASK)
            | ((slot as u64 & Self::SLOT_MASK) << Self::SLOT_SHIFT)
            | ((tag as u64 & Self::TAG_MASK) << Self::TAG_SHIFT);
        Self(raw)
    }

    pub(super) fn unpack(self) -> (u8, u8, u32, u32) {
        let epoch = (self.0 & Self::EPOCH_MASK) as u32;
        let slot = ((self.0 >> Self::SLOT_SHIFT) & Self::SLOT_MASK) as u32;
        let tag = ((self.0 >> Self::TAG_SHIFT) & Self::TAG_MASK) as u8;
        let route = ((self.0 >> Self::ROUTE_SHIFT) & Self::ROUTE_MASK) as u8;
        (tag, route, slot, epoch)
    }

    pub(super) fn from_kevent(p: *mut libc::c_void) -> Self {
        Self(p as usize as u64)
    }

    pub(crate) fn into_kevent(self) -> *mut libc::c_void {
        self.0 as usize as *mut libc::c_void
    }

    pub(super) fn from_token(token: Token, tag: u8) -> Self {
        let base = Self::pack(tag, token.slot().raw(), token.epoch().raw());
        Self(base.0 | ((token.route() as u64 & Self::ROUTE_MASK) << Self::ROUTE_SHIFT))
    }
}
