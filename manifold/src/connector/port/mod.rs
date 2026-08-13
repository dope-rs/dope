use std::marker;

use dope_core::driver::schedule::{self, ready};
use dope_net::link::egress::{self, data, metadata::arena};
use o3::{cell::region, collections};

use crate::{
    connector::{app, connection},
    dispatch::typed::identity,
};

mod batch;
mod receiver;
mod retirement;
mod sender;
mod state;

pub use batch::Batch;
pub use sender::Sender;

type ConnectionId<'d, const ID: u8> = connection::Id<'d, ID>;
type EntryTransaction<'a, 'd, B, I> = (
    arena::Slot<'a, 'd, B, state::Entry<'d, I>>,
    state::Transaction<I>,
);

/// A connector-side request port bound to one exact driver route.
///
/// A connection from another route cannot enter this port:
///
/// ```compile_fail,E0308
/// use dope_manifold::connector::{connection, port::Port};
/// use o3::cell::region;
///
/// fn cross_route<'d>(
///     port: &Port<'d, &'static [u8], 1>,
///     region: &mut region::Token<'d>,
///     connection: connection::Id<'d, 2>,
/// ) {
///     let _ = port.try_enqueue(region, connection, b"payload");
/// }
/// ```
pub struct Port<'d, B, const ID: u8 = 0> {
    core: Core<'d, B, ConnectionId<'d, ID>>,
}

/// Construction-time retained staging policy for one payload type.
pub struct Plan<B> {
    lanes: usize,
    retained: egress::Config,
    payload: marker::PhantomData<fn(B) -> B>,
}

impl<B> Clone for Plan<B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<B> Copy for Plan<B> {}

impl<B> Plan<B> {
    pub const fn new(lanes: usize, retained: egress::Config) -> Option<Self> {
        if lanes == 0 || retained.entry_capacity() == 0 {
            return None;
        }
        Some(Self {
            lanes,
            retained,
            payload: marker::PhantomData,
        })
    }

    pub const fn capacity(&self) -> usize {
        self.lanes
    }

    pub const fn retained(&self) -> egress::Config {
        self.retained
    }
}

struct Core<'d, B, I: identity::Identity> {
    arena: arena::Arena<'d, B, state::Entry<'d, I>>,
}

impl<'d, B: data::Payload<'d>, I: identity::Identity> Core<'d, B, I> {
    fn try_from_plan(
        plan: Plan<B>,
        token: &region::Token<'d>,
    ) -> Result<Self, collections::AllocationError> {
        use arena::Arena;
        Ok(Self {
            arena: Arena::try_with_slots(token, plan.retained, plan.lanes, state::Entry::new)?,
        })
    }

    fn capacity(&self) -> usize {
        self.arena.len()
    }

    fn is_active(&self, connection: I) -> bool {
        self.entry_transaction(connection).is_some()
    }

    fn entry_transaction(&self, connection: I) -> Option<EntryTransaction<'_, 'd, B, I>> {
        let slot = self.arena.get(connection.index())?;
        Some((slot, slot.state().transaction(connection)?))
    }

    fn activate(&self, connection: I, ready: ready::Target<'d>) -> bool {
        let Some(slot) = self.arena.get(connection.index()) else {
            return false;
        };
        slot.state()
            .activate(connection, ready, || slot.queue().is_empty())
    }

    fn retire<'turn>(
        &self,
        connection: I,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> egress::ClearProgress {
        let Some(slot) = self.arena.get(connection.index()) else {
            return egress::ClearProgress::Done;
        };
        let Some(retirement) = retirement::Retirement::begin(slot, connection, region) else {
            return egress::ClearProgress::Done;
        };
        retirement.clear(work)
    }

    fn try_enqueue(
        &self,
        region: &mut region::Token<'d>,
        connection: I,
        value: B,
    ) -> Result<(), B> {
        let Some((slot, transaction)) = self.entry_transaction(connection) else {
            return Err(value);
        };
        let entry = slot.state();
        if !entry.is_active(transaction) {
            return Err(value);
        }
        if value.as_ref().is_empty() {
            drop(value);
            return Ok(());
        }
        slot.queue().try_push_back(region, value)?;
        entry.mark_ready(transaction);
        Ok(())
    }

    fn close(&self, connection: I) {
        let Some(slot) = self.arena.get(connection.index()) else {
            return;
        };
        slot.state().close(connection);
    }

    fn drain_requests(
        &self,
        region: &mut region::Token<'d>,
        connection: I,
        drain: &mut app::RequestDrain<'_, 'd, B>,
        begin: impl FnMut(),
    ) -> Option<app::Requests> {
        let (slot, transaction) = self.entry_transaction(connection)?;
        let receiver = receiver::Receiver::new(slot, transaction);
        use crate::connector::app::CloseKind;
        let close = receiver.take_close();
        if !close {
            receiver.drain(region, drain, begin);
        }
        Some(app::Requests {
            close: close.then_some(CloseKind::Reconnect),
        })
    }
}

impl<'d, B: data::Payload<'d>, const ID: u8> Port<'d, B, ID> {
    pub fn try_new(
        plan: Plan<B>,
        token: &region::Token<'d>,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            core: Core::try_from_plan(plan, token)?,
        })
    }

    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    pub fn is_active(&self, connection: ConnectionId<'d, ID>) -> bool {
        self.core.is_active(connection)
    }

    pub fn with_sender<R>(
        &self,
        connection: ConnectionId<'d, ID>,
        f: impl for<'a> FnOnce(Sender<'a, 'd, B, ID>) -> R,
    ) -> Option<R> {
        let (slot, transaction) = self.core.entry_transaction(connection)?;
        Some(f(Sender::new(slot, transaction)))
    }

    pub fn activate(&self, connection: ConnectionId<'d, ID>, ready: ready::Target<'d>) -> bool {
        self.core.activate(connection, ready)
    }

    pub fn retire<'turn>(
        &self,
        connection: ConnectionId<'d, ID>,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    ) -> egress::ClearProgress {
        self.core.retire(connection, work, region)
    }

    pub fn try_enqueue(
        &self,
        region: &mut region::Token<'d>,
        connection: ConnectionId<'d, ID>,
        value: B,
    ) -> Result<(), B> {
        self.core.try_enqueue(region, connection, value)
    }

    pub fn close(&self, connection: ConnectionId<'d, ID>) {
        self.core.close(connection);
    }

    pub fn drain_requests(
        &self,
        region: &mut region::Token<'d>,
        connection: ConnectionId<'d, ID>,
        drain: &mut app::RequestDrain<'_, 'd, B>,
        begin: impl FnMut(),
    ) -> Option<app::Requests> {
        self.core.drain_requests(region, connection, drain, begin)
    }
}
