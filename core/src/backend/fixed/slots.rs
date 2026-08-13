use std::{io, marker, mem, process};

use o3::collections::fixed::index;

use crate::{
    driver::{self, route, schedule::ready, settings},
    io::fd::handles,
};

type Invariant<'d> = marker::PhantomData<fn(&'d ()) -> &'d ()>;

pub(crate) struct Slots {
    floor: u32,
    pool: index::Pool,
}

impl Slots {
    pub(crate) fn new(layout: settings::FileSlots) -> io::Result<Self> {
        let capacity = layout.outbound();
        Ok(Self {
            floor: layout.accept(),
            pool: index::Pool::try_with_capacity(capacity)?,
        })
    }

    pub(crate) fn alloc_slot<'d>(
        &mut self,
        _driver: driver::Reference<'d>,
    ) -> io::Result<Slot<'d>> {
        self.take().map(|slot| Slot {
            slot,
            _brand: marker::PhantomData,
        })
    }

    pub(crate) fn alloc<'d>(
        &mut self,
        len: u32,
        _driver: driver::Reference<'d>,
    ) -> io::Result<Reservation<'d>> {
        use o3::collections::BoxSliceExt;

        if len == 0 || len > self.pool.available() {
            return Err(exhausted());
        }
        let slots =
            BoxSliceExt::try_box_with(len as usize, |_| self.take_available()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "dope: fixed-file reservation storage unavailable",
                )
            })?;
        Ok(Reservation {
            allocation: Allocation(slots),
            _brand: marker::PhantomData,
        })
    }

    pub(crate) fn release(&mut self, reservation: Reservation<'_>) {
        for slot in reservation.allocation.0.iter().copied() {
            self.release_allocated(slot);
        }
    }

    pub(crate) fn release_slot(&mut self, slot: Slot<'_>) {
        self.release_allocated(slot.slot);
    }

    fn take(&mut self) -> io::Result<handles::FixedSlot> {
        match self.pool.take() {
            Some(relative) => Ok(self.slot_for_relative(relative)),
            None => Err(exhausted()),
        }
    }

    fn take_available(&mut self) -> handles::FixedSlot {
        let Some(relative) = self.pool.take() else {
            process::abort();
        };
        self.slot_for_relative(relative)
    }

    fn slot_for_relative(&self, relative: u32) -> handles::FixedSlot {
        let raw = self.floor + relative;
        handles::FixedSlot::from_index(route::SlotIndex::from_bounded(raw))
    }

    fn release_allocated(&mut self, slot: handles::FixedSlot) {
        debug_assert!(slot.raw() >= self.floor);
        let relative = slot.raw().wrapping_sub(self.floor);
        if !self.pool.release(relative) {
            process::abort();
        }
    }
}

fn exhausted() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "dope: fixed-file slots exhausted",
    )
}

#[repr(transparent)]
pub(crate) struct Slot<'d> {
    pub(super) slot: handles::FixedSlot,
    pub(super) _brand: Invariant<'d>,
}

#[repr(transparent)]
pub(crate) struct Retirement<'d> {
    pub(super) slot: handles::FixedSlot,
    pub(super) _brand: Invariant<'d>,
}

#[repr(transparent)]
pub(crate) struct Allocation(pub(super) Box<[handles::FixedSlot]>);

#[repr(transparent)]
pub(crate) struct Reservation<'d> {
    pub(super) allocation: Allocation,
    pub(super) _brand: Invariant<'d>,
}

impl Allocation {
    pub(crate) fn len(&self) -> u32 {
        self.0.len() as u32
    }

    pub(crate) fn get(&self, local: route::SlotIndex) -> Option<handles::FixedSlot> {
        self.0.get(local.raw() as usize).copied()
    }
}

impl<'d> Slot<'d> {
    pub(crate) fn fixed(&self) -> handles::FixedSlot {
        self.slot
    }

    pub(crate) fn retire(self) -> Retirement<'d> {
        Retirement {
            slot: self.slot,
            _brand: marker::PhantomData,
        }
    }

    pub(crate) fn into_claimed(self, key: ready::FixedKey<'d>) -> handles::FixedSlot {
        debug_assert_eq!(self.slot.raw(), key.index());
        self.slot
    }
}

impl<'d> Retirement<'d> {
    pub(crate) fn from_release(released: ready::FixedRelease<'d>) -> Self {
        Self {
            slot: released.into_slot(),
            _brand: marker::PhantomData,
        }
    }

    pub(crate) fn into_slot(self) -> Slot<'d> {
        Slot {
            slot: self.slot,
            _brand: marker::PhantomData,
        }
    }

    pub(crate) fn into_raw(self) -> super::raw::Retirement {
        super::raw::Retirement::new(self.slot)
    }
}

impl Reservation<'_> {
    pub(crate) fn into_allocation(self) -> Allocation {
        self.allocation
    }

    pub(crate) fn len(&self) -> u32 {
        self.allocation.len()
    }
}

impl<'d> Reservation<'d> {
    pub(crate) fn from_retired(allocation: Allocation, _retired: driver::RetiredSlots<'d>) -> Self {
        Self {
            allocation,
            _brand: marker::PhantomData,
        }
    }
}

const _: () = assert!(mem::size_of::<Allocation>() == mem::size_of::<Box<[handles::FixedSlot]>>());
const _: () = assert!(mem::size_of::<Slots>() == mem::size_of::<(Box<[u32]>, [u32; 3])>());
const _: () = assert!(mem::size_of::<Slot<'static>>() == mem::size_of::<handles::FixedSlot>());
const _: () =
    assert!(mem::size_of::<Retirement<'static>>() == mem::size_of::<handles::FixedSlot>());
const _: () =
    assert!(mem::size_of::<Reservation<'static>>() == mem::size_of::<Box<[handles::FixedSlot]>>());
