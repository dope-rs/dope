use std::mem;

use dope_core::driver::{
    flight, route,
    schedule::{self, ready},
};
use dope_net::{
    link::{egress, pool, slot::types},
    wire,
};

use crate::{
    dispatch::typed::{self, identity},
    listener::{
        self,
        writer::{self, flow, flow::SlotFlow as _},
    },
};

mod write;

pub use write::Write;

struct Kind;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Id<'d, const ID: u8>(typed::Id<'d, ID, Kind>);

const _: () = assert!(mem::size_of::<Id<'static, 0>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Id<'static, 0>>() == mem::align_of::<route::Token>());

impl<'d, const ID: u8> Id<'d, ID> {
    pub(in crate::listener) fn from_key(key: pool::Key<'d, ID>) -> Self {
        Self(typed::Id::from_key(key))
    }

    pub(in crate::listener) const fn key(self) -> pool::Key<'d, ID> {
        self.0.key
    }

    pub const fn index(self) -> usize {
        self.0.key.index()
    }
}

impl<const ID: u8> identity::Brand for Id<'_, ID> {}

impl<const ID: u8> identity::Identity for Id<'_, ID> {
    fn index(self) -> usize {
        self.0.key.index()
    }
}

mod state;

pub(crate) use state::State;

pub struct Ref<'a, 'd, const ID: u8, W: wire::Wire, C> {
    slot: &'a types::Connection<'d, ID, W, State<'d, ID, C>>,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, C> Ref<'a, 'd, ID, W, C> {
    pub(super) const fn new(slot: &'a types::Connection<'d, ID, W, State<'d, ID, C>>) -> Self {
        Self { slot }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(typed::Id::from_key(self.slot.key()))
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }
}

pub struct Ctx<'a, 'd, const ID: u8, W: wire::Wire, C> {
    slot: &'a mut types::Connection<'d, ID, W, State<'d, ID, C>>,
    output: Output<'a, 'd, ID>,
    work: schedule::Application<'a, 'd>,
}

enum Output<'a, 'd, const ID: u8> {
    Open {
        flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
        retention: writer::Retention<'a, 'd, ID>,
        queue: egress::Queue<'a, 'd, { listener::IOV_CAP }, writer::Payload<'d, ID>>,
    },
    Sealed,
}

impl<'a, 'd, const ID: u8, W: wire::Wire, C> Ctx<'a, 'd, ID, W, C> {
    pub(super) const fn new(
        slot: &'a mut types::Connection<'d, ID, W, State<'d, ID, C>>,
        flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
        retention: writer::Retention<'a, 'd, ID>,
        queue: egress::Queue<'a, 'd, { listener::IOV_CAP }, writer::Payload<'d, ID>>,
        work: schedule::Application<'a, 'd>,
    ) -> Self {
        Self {
            slot,
            output: Output::Open {
                flights,
                retention,
                queue,
            },
            work,
        }
    }

    pub(super) const fn sealed(
        slot: &'a mut types::Connection<'d, ID, W, State<'d, ID, C>>,
        work: schedule::Application<'a, 'd>,
    ) -> Self {
        Self {
            slot,
            output: Output::Sealed,
            work,
        }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(typed::Id::from_key(self.slot.key()))
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.slot.state.conn
    }

    pub fn set_close_after(&mut self) {
        self.slot.set_close_after();
    }

    pub fn begin_discard(&mut self, bytes: usize) -> bool {
        self.slot.begin_discard(bytes)
    }

    pub fn is_send_inflight(&self) -> bool {
        self.slot.send_status().inflight()
    }

    pub fn has_pending_egress(&self) -> bool {
        let deferred = match &self.output {
            Output::Open { queue, .. } => queue.total_bytes() != 0,
            Output::Sealed => false,
        };
        !matches!(self.slot.flow(deferred), flow::Flow::Clear)
    }

    /// Reborrows the exact application-turn capability carried by this
    /// callback context.
    pub fn application_work(&self) -> schedule::Application<'a, 'd> {
        self.work
    }

    /// Returns a driver- and generation-branded target for deferred wakeups.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.slot.io().wake_target()
    }

    /// Reserves a domain-branded pinned header slot, returning `None` on
    /// bounded header backpressure without allocation or copying.
    pub fn try_write(&mut self) -> Option<Write<'_, 'd, ID, W, C>> {
        let slot = &mut *self.slot;
        let Output::Open {
            flights,
            retention,
            queue,
        } = &mut self.output
        else {
            return None;
        };
        let deferred = queue.total_bytes() != 0;
        let queued = !matches!(slot.flow(deferred), flow::Flow::Clear);
        let buffer = retention.reserve(queued, queue.reborrow())?;
        Some(Write::new(slot, flights, buffer, self.work))
    }
}
