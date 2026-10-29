use std::mem;

use crate::driver::route;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(super) struct Phase(u8);

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub(super) struct SlotState(u16);

impl SlotState {
    pub(super) const FREE: Self = Self(0);
    pub(super) const STANDALONE: Self = Self((Phase::STANDALONE.raw() as u16) << u8::BITS);

    pub(super) const fn route(phase: Phase, owner: route::OutboundKey) -> Self {
        Self(((phase.raw() as u16) << u8::BITS) | owner.raw() as u16)
    }

    pub(super) const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    pub(super) const fn raw(self) -> u16 {
        self.0
    }

    pub(super) const fn phase(self) -> Phase {
        Phase((self.0 >> u8::BITS) as u8)
    }

    pub(super) fn owner(self) -> Option<route::OutboundKey> {
        let phase = self.phase();
        if matches!(phase, Phase::FREE | Phase::STANDALONE) {
            None
        } else {
            route::OutboundKey::from_raw(u32::from(self.0 as u8))
        }
    }

    pub(super) fn is_state(self, phase: Phase, owner: route::OutboundKey) -> bool {
        self.phase() == phase && self.owner() == Some(owner)
    }
}

impl Phase {
    pub(super) const FREE: Self = Self(0);
    pub(super) const STANDALONE: Self = Self(1);
    pub(super) const MAPPED: Self = Self(2);
    pub(super) const UNOPENED_ONE: Self = Self(3);
    pub(super) const CREATING_ONE: Self = Self(4);
    pub(super) const CREATING_ZERO: Self = Self(5);
    pub(super) const OPEN_ONE: Self = Self(6);
    pub(super) const OPEN_TWO: Self = Self(7);
    pub(super) const FAILED_ONE: Self = Self(8);
    pub(super) const CLOSING_ZERO: Self = Self(9);
    pub(super) const CLOSING_ONE: Self = Self(10);
    pub(super) const CLOSED_ONE: Self = Self(11);

    const fn raw(self) -> u8 {
        self.0
    }
}

const _: () = {
    assert!(mem::size_of::<Phase>() == mem::size_of::<u8>());
    assert!(mem::size_of::<SlotState>() == mem::size_of::<u16>());
};
