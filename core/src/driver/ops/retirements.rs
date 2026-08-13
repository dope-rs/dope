use crate::{
    backend::fixed,
    driver::{self, ops, schedule, storage::retirements},
};

pub(crate) enum Retirement {}

pub(in crate::driver) struct Reclaimer<'a, 'scope, 'd, const TERMINAL: bool> {
    context: &'a mut driver::Context<'scope, 'd>,
}

impl<'turn, 'd> schedule::Budget<'turn, 'd, Retirement> {
    pub(crate) fn reclaim(&mut self, context: &mut driver::Context<'_, 'd>) {
        let mut reclaimer = Reclaimer::<false>::new(context);
        while let schedule::Admission::Item(record) = self.admit_with(|| reclaimer.pop()) {
            reclaimer.record(record);
        }
    }
}

impl<'a, 'scope, 'd, const TERMINAL: bool> Reclaimer<'a, 'scope, 'd, TERMINAL> {
    const PHASE: fixed::Phase = if TERMINAL {
        fixed::Phase::Final
    } else {
        fixed::Phase::Active
    };

    pub(in crate::driver) fn new(context: &'a mut driver::Context<'scope, 'd>) -> Self {
        Self { context }
    }

    pub(in crate::driver) fn all(&mut self) {
        while let Some(record) = self.pop() {
            self.record(record);
        }
    }

    fn pop(&self) -> Option<retirements::Record<'d>> {
        let driver = self.context.driver_ref();
        driver.maintenance().pop_deferred_retirement()
    }

    fn record(&mut self, record: retirements::Record<'d>) {
        let context = &mut *self.context;
        match record {
            retirements::Record::Route(id) => {
                ops::Control::release_route(context, id);
            }
            retirements::Record::Retire(retired) => {
                fixed::Lifecycle::retire(context.backend(), retired.into_slot(), Self::PHASE);
            }
            retirements::Record::Descriptor { slot, outbound } => {
                let driver = context.driver_ref();
                match driver.outbound().close_disposition(slot, outbound) {
                    driver::CloseDisposition::Submit(close) => {
                        fixed::Lifecycle::close(context.backend(), close, driver, Self::PHASE);
                    }
                    driver::CloseDisposition::NoSubmit(Some(retired)) => {
                        let slots = driver.outbound().take_retired_slots(retired);
                        fixed::Lifecycle::release_slots(context.backend(), slots);
                    }
                    driver::CloseDisposition::NoSubmit(None) => {}
                }
            }
            retirements::Record::Close { slot } => {
                let driver = context.driver_ref();
                let close = driver::Close::untracked(slot);
                fixed::Lifecycle::close(context.backend(), close, driver, Self::PHASE);
            }
            retirements::Record::OutboundSlots { slots: retired } => {
                let driver = context.driver_ref();
                let slots = driver.outbound().take_retired_slots(retired);
                fixed::Lifecycle::release_slots(context.backend(), slots);
            }
        }
    }
}
