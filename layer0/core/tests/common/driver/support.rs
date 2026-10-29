use std::{iter, mem, num};

use dope_core::{
    driver,
    driver::{
        route::{Epoch, Identity, KeyTag, Operation, Target, indices::SlotIndex},
        schedule::app,
        settings,
        settings::demands::Schedule,
    },
};
use settings::demands::{Overflow, slots};

pub const ROUTE: u8 = 7;

pub trait Stamp {
    fn epoch(self) -> Epoch;
}

impl Stamp for num::NonZero<u64> {
    fn epoch(self) -> Epoch {
        // SAFETY: Epoch is transparent over NonZero<u64> and a ThreadBound ZST;
        // every nonzero stamp is valid, including the terminal generation.
        unsafe { mem::transmute(self) }
    }
}

pub trait Drain {
    fn drain_count(self) -> usize;
}

impl Drain for app::Work<'_, '_> {
    fn drain_count(self) -> usize {
        iter::from_fn(|| self.permit()).count()
    }
}

pub trait Targets<'d> {
    fn tuned_target<ID: Identity>(self, slot: SlotIndex) -> Target<'d, ID>;
    fn target(self) -> Operation<'d, KeyTag<ROUTE>>;
}

impl<'d> Targets<'d> for driver::Reference<'d> {
    fn tuned_target<ID: Identity>(self, slot: SlotIndex) -> Target<'d, ID> {
        self.targets::<ID>().bind(slot, Epoch::INITIAL)
    }

    fn target(self) -> Operation<'d, KeyTag<ROUTE>> {
        self.targets::<KeyTag<ROUTE>>()
            .bind(SlotIndex::ZERO, Epoch::INITIAL)
            .dispatch()
    }
}

pub trait Configure {
    fn with_ready(self, slots: usize) -> Result<settings::Config, Overflow>;
}

impl Configure for settings::Config {
    fn with_ready(self, slots: usize) -> Result<settings::Config, Overflow> {
        let slots = slots::Slots::try_from(slots)?;
        let schedule = settings::Schedule::fixed::<0, 0>()
            .resolve(Schedule::new(slots, self.schedule().timers()));
        Ok(self.set_schedule(schedule))
    }
}
