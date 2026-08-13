use std::io;

use crate::driver::{self, flight};

pub(crate) mod raw;
mod slots;

pub(crate) use slots::{Allocation, Reservation, Retirement, Slot, Slots};

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Active,
    Final,
}

pub(crate) trait Lifecycle {
    fn alloc_slots<'d>(
        &mut self,
        len: u32,
        driver: driver::Reference<'d>,
    ) -> io::Result<Reservation<'d>>;

    fn release_slots<'d>(&mut self, slots: Reservation<'d>);

    fn close<'d>(&mut self, close: driver::Close<'d>, driver: driver::Reference<'d>, phase: Phase);

    fn retire<'d>(&mut self, slot: Slot<'d>, phase: Phase);
}

pub(crate) trait Finalize: Lifecycle {
    fn settle<'q, 'd>(&mut self, drain: flight::Drain<'q, 'd>) -> io::Result<()>;
}
