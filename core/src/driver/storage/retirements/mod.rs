use crate::{backend::fixed, driver, io::fd::handles};

mod sealed;

pub(in crate::driver) use sealed::Queue;

pub(in crate::driver) enum Record<'d> {
    Route(u8),
    Descriptor {
        slot: handles::FixedSlot,
        outbound: Option<driver::OutboundKey>,
    },
    Retire(fixed::Retirement<'d>),
    Close {
        slot: handles::FixedSlot,
    },
    OutboundSlots {
        slots: driver::RetiredSlots<'d>,
    },
}
