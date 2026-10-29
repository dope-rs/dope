pub(in crate::driver::schedule::clock::timer::heap) mod lease;
pub(in crate::driver::schedule::clock::timer::heap) mod state;

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Slot(u32);

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(super) struct Live(usize);

impl Live {
    pub(super) const fn from_slot(slot: Slot) -> Self {
        Self(slot.offset())
    }

    pub(super) const fn slot(self) -> Slot {
        Slot(self.0 as u32)
    }
}

impl Slot {
    pub(super) const fn from_stored(raw: u32) -> Self {
        Self(raw)
    }

    pub(super) const fn offset(self) -> usize {
        self.0 as usize
    }

    pub(super) const fn raw(self) -> u32 {
        self.0
    }
}

const _: () = {
    assert!(std::mem::size_of::<Live>() == std::mem::size_of::<usize>());
    assert!(std::mem::size_of::<Slot>() == std::mem::size_of::<u32>());
};
