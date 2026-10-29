use dope_core::driver::{
    receive::credit,
    route::{SLOT_MASK, table::conn},
    settings,
};
pub use loan::Loan;
use o3::depot::queue::pool;
pub(in crate::link::pool::recv) use seal::Seal;

use crate::link::event;

mod loan;
pub mod queue;
pub mod raw;
mod seal;
pub mod tail;

pub enum Extent {
    Empty,
    Lanes(usize),
}

pub struct Parking<'d> {
    pub(crate) events: pool::Lanes<event::Completion<'d>>,
}

impl<'d> Parking<'d> {
    pub(crate) fn try_new(conns: conn::Capacity, bufs: settings::Buffers) -> crate::Result<Self> {
        let conns = conns.entries().get();
        let events = usize::from(bufs.into_nonzero().get()) + conns;
        let plan = pool::Plan::new(events, conns)
            .map_err(crate::ConfigError::ReceiveLayout)
            .map_err(crate::Resource::Config)?;
        Ok(Self {
            events: pool::Lanes::try_new(plan).map_err(crate::Resource::Allocation)?,
        })
    }
}

pub struct Retained<'d> {
    pub(crate) events: Parking<'d>,
    pub(crate) credit: credit::Storage<'d>,
}

impl<'d> Retained<'d> {
    pub(crate) fn try_new(
        conns: conn::Capacity,
        bufs: settings::Buffers,
        credit: credit::Storage<'d>,
    ) -> crate::Result<Self> {
        Ok(Self {
            events: Parking::try_new(conns, bufs)?,
            credit,
        })
    }
}

const _: () = assert!(
    (SLOT_MASK as usize)
        .checked_add(u16::MAX as usize)
        .is_some()
);
