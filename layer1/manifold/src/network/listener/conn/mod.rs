use std::{marker, mem};

use data::{compose, exec};
use dope_core::{
    driver::{
        result,
        retained::flight,
        route,
        route::{Identity, KeyTag, indices},
        schedule::{app, ready, ready::task::owner},
    },
    io::transfer,
};
use dope_net::{
    link::{
        egress::{self, data},
        event,
        pool::{self, change::close, pending},
        slot::{transmit, types},
    },
    wire::{
        self,
        receive::{self, discard, mode},
    },
};
use o3::{base::num::bounded, buffer::resident};

use crate::{
    app::dispatch::identity,
    network::listener::{
        self,
        writer::{deadline::Slot, flow, memory},
    },
};

pub mod write;
use write::body;

pub mod deadline;

pub enum Failure {
    Option(dope_core::Errno),
    Submit(result::SubmitError),
    Tuning(dope_core::Errno),
    Receive(dope_core::Errno),
    Application(crate::app::receive::Failure),
    Park(event::ParkError),
    Write(body::Failure),
    Timeout(deadline::Kind),
    Deadline(deadline::Error),
}

#[derive(Clone, Copy)]
pub enum Transmit {
    Io(event::SendFailure<dope_core::Errno>),
    Invalid(transmit::Invalid),
    Rejected(event::SendFailure<result::SubmitError>),
    Payload {
        offset: transfer::Len,
        length: transfer::Len,
        remaining: transfer::Len,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Id<'d, ID: Identity>(pool::Key<'d, ID>);

impl<ID: Identity> owner::Identity for Id<'_, ID> {
    fn parts(self) -> (owner::Lane, owner::Generation) {
        (
            owner::Lane::from(self.0.lane()),
            owner::Generation::from_route(self.0.epoch()),
        )
    }
}

impl<'d, ID: Identity> Id<'d, ID> {
    pub(in crate::network::listener) fn from_key(key: pool::Key<'d, ID>) -> Self {
        Self(key)
    }

    pub(in crate::network::listener) const fn key(self) -> pool::Key<'d, ID> {
        self.0
    }

    pub const fn index(self) -> bounded::U32<0, { route::SLOT_MASK as u32 }> {
        self.0.index()
    }
}

impl<ID: Identity> identity::Identity for Id<'_, ID> {
    fn slot(self) -> indices::SlotIndex {
        self.0.lane()
    }
}

pub(in crate::network::listener) mod state;

pub(in crate::network::listener) use state::State;

pub struct Ref<
    'a,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
> {
    control: &'a types::Control<'w, 'd, ID, W>,
    state: &'a C,
    retained: marker::PhantomData<&'a state::Control<'p, 's, 'd, ID, B>>,
}

impl<'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B: data::raw::Payload<'d>>
    Ref<'a, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub(super) fn new(
        slot: &'a types::Connection<'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
    ) -> Self {
        Self {
            control: slot,
            state: &slot.state.conn,
            retained: marker::PhantomData,
        }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(self.control.key())
    }

    pub fn state(&self) -> &C {
        self.state
    }

    /// Returns whether the wire has finished terminal convergence.
    /// Application work may still require closure to remain deferred.
    pub fn is_terminal(&self) -> bool {
        self.control.terminal.is_done()
    }

    pub fn map_state<T>(
        self,
        select: impl FnOnce(&'a C) -> &'a T,
    ) -> Ref<'a, 'p, 's, 'w, 'd, ID, W, T, B> {
        Ref {
            control: self.control,
            state: select(self.state),
            retained: self.retained,
        }
    }
}

/// Final owned state after the connection slot and every egress owner retire.
pub struct Retired<
    'p,
    's,
    'd,
    ID: Identity,
    W: wire::Spec,
    C,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
> {
    id: Id<'d, ID>,
    state: C,
    failure: Option<Failure>,
    transmit: Option<Transmit>,
    wire: marker::PhantomData<(W, state::Control<'p, 's, 'd, ID, B>)>,
}

/// Affine commit authority after every egress owner has drained.
pub struct Drained<
    'e,
    'a,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d> + 'd = resident::Shared<'d>,
> {
    control: &'a mut types::Control<'w, 'd, ID, W>,
    state: &'a mut C,
    output: marker::PhantomData<Output<'a, 'p, 's, 'e, 'd, ID, B>>,
}

impl<
    'e,
    'a,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d> + 'd,
> Drained<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub fn map_state<T>(
        self,
        select: impl for<'view> FnOnce(&'view mut C) -> &'view mut T,
    ) -> Drained<'e, 'a, 'p, 's, 'w, 'd, ID, W, T, B> {
        Drained {
            control: self.control,
            state: select(self.state),
            output: self.output,
        }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(self.control.key())
    }

    pub fn state(&self) -> &C {
        self.state
    }

    pub fn state_mut(&mut self) -> &mut C {
        self.state
    }

    /// Records a close intent without acquiring output or scheduling
    /// authority. The listener observes it after the drain callback returns.
    pub fn close_after(&mut self) {
        self.control.close_after();
    }
}

impl<'p, 's, 'd, ID: Identity, W: wire::Spec, C, B: data::raw::Payload<'d>>
    Retired<'p, 's, 'd, ID, W, C, B>
{
    pub fn map_state<T>(self, select: impl FnOnce(C) -> T) -> Retired<'p, 's, 'd, ID, W, T, B> {
        Retired {
            id: self.id,
            state: select(self.state),
            failure: self.failure,
            transmit: self.transmit,
            wire: self.wire,
        }
    }

    pub(in crate::network::listener) fn take(
        key: pool::Key<'d, ID>,
        mut state: State<'p, 's, 'd, ID, C, B>,
        kind: close::Kind,
    ) -> (Option<B>, Option<Self>) {
        let (egress, transmit) = state.send.retire();
        let conn = match kind {
            close::Kind::Connected => Some(Self {
                id: Id::from_key(key),
                state: state.conn,
                failure: state.control.failure,
                transmit,
                wire: marker::PhantomData,
            }),
            close::Kind::Opening => None,
        };
        (egress, conn)
    }

    pub fn id(&self) -> Id<'d, ID> {
        self.id
    }

    pub fn state(&self) -> &C {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.state
    }

    /// Exact listener lifecycle failure retained until the final owner audit.
    pub const fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    pub const fn transmit(&self) -> Option<&Transmit> {
        self.transmit.as_ref()
    }

    /// Exact resident response bytes still owned by the listener transport.
    /// Construction is possible only after both queued and direct owners were
    /// retired, so this is a proof-bearing zero rather than an assumed zero.
    pub const fn response_len(&self) -> usize {
        0
    }
}

pub struct Ctx<
    'e,
    'a,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B = resident::Shared<'d>,
> where
    B: data::raw::Payload<'d>,
{
    pub(super) slot: Slot<'a, 'p, 's, 'w, 'd, ID, W, C, B>,
    pub(super) output: Output<'a, 'p, 's, 'e, 'd, ID, B>,
    pub(super) work: app::Work<'a, 'd>,
}

pub(super) enum Output<'a, 'p, 's, 'e, 'd, ID: Identity, B: data::raw::Payload<'d>> {
    Open {
        slots: &'a flight::Slots<'d, ID>,
        permit: pending::Handle<'a, 'd, ID>,
        held: memory::Retention<'p, 's, 'd, ID, B>,
        queue: egress::Queue<
            'a,
            'e,
            'd,
            { listener::IOV_CAP },
            compose::Either<listener::raw::Owned<'p, 's, 'd, ID>, B>,
            <B::Mode as exec::Mode>::Reserve<{ memory::WRITE_BUF_CAP }>,
        >,
    },
    Sealed,
}

impl<'p, 's, 'e, 'd, ID: Identity, B: data::raw::Payload<'d>> Output<'_, 'p, 's, 'e, 'd, ID, B> {
    fn reborrow(&self) -> Output<'_, 'p, 's, 'e, 'd, ID, B> {
        match self {
            Self::Open {
                slots,
                permit,
                held,
                queue,
            } => Output::Open {
                slots,
                permit: *permit,
                held: held.reborrow(),
                queue: queue.reborrow(),
            },
            Self::Sealed => Output::Sealed,
        }
    }

    fn pending<'w, W: wire::Spec + 'w + 'd, C>(
        &self,
        slot: &Slot<'_, 'p, 's, 'w, 'd, ID, W, C, B>,
    ) -> egress::Pending
    where
        'd: 'w,
    {
        let pending = match self {
            Self::Open { queue, .. } => queue.pending(),
            Self::Sealed => egress::Pending::Absent,
        };
        if matches!(slot.flow(pending), flow::Flow::Clear) {
            egress::Pending::Absent
        } else {
            egress::Pending::Present
        }
    }
}

impl<
    'e,
    'a,
    'p,
    's,
    'w,
    'd: 'w,
    ID: Identity,
    W: wire::Spec + 'w + 'd,
    C,
    B: data::raw::Payload<'d>,
> Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>
{
    /// Reborrows child state.
    pub fn map_state<'view, T>(
        &'view mut self,
        select: impl for<'child> FnOnce(&'child mut C) -> &'child mut T,
    ) -> Ctx<'e, 'view, 'p, 's, 'w, 'd, ID, W, T, B> {
        Ctx {
            slot: self.slot.project(select),
            output: self.output.reborrow(),
            work: self.work,
        }
    }

    pub(super) fn sealed(
        slot: Slot<'a, 'p, 's, 'w, 'd, ID, W, C, B>,
        work: app::Work<'a, 'd>,
    ) -> Self {
        Self {
            slot,
            output: Output::Sealed,
            work,
        }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(self.slot.key())
    }

    pub fn state(&self) -> &C {
        self.slot.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        self.slot.conn
    }

    /// Exact listener lifecycle failure retained by this connection.
    pub fn failure(&self) -> Option<&Failure> {
        self.slot.state.failure.as_ref()
    }

    /// Returns the first exact send failure without changing its retained owner.
    pub fn transmit(&self) -> Option<Transmit> {
        self.slot.state.send.error()
    }

    pub fn close_after(&mut self) {
        self.slot.close_after();
    }

    pub fn deadline(&mut self) -> deadline::Deadline<'e, '_, 'a, 'p, 's, 'w, 'd, ID, W, C, B> {
        deadline::Deadline::new(self)
    }

    /// Orders a write-half shutdown after every already-admitted payload and
    /// wire trailer. The read half and exact connection generation remain live.
    pub fn shutdown(&mut self) {
        self.slot.shutdown();
    }

    /// Attempts to discard an exact prefix of future raw receive bytes.
    /// Unsupported wires return the exact prefix for application-level consumption.
    pub fn try_discard(&mut self, prefix: discard::Prefix) -> discard::Start {
        self.slot.try_discard(prefix)
    }

    /// Discards an exact prefix of future raw receive bytes.
    ///
    /// Available only when the wire's receive policy enables raw discard.
    pub fn discard(&mut self, prefix: discard::Prefix) -> discard::Start
    where
        W::Receive: receive::Policy<W, Discard = mode::Enabled>,
    {
        self.slot.try_discard(prefix)
    }

    /// Consumes a listener send callback at its exact egress-drain boundary.
    pub(super) fn try_drain(self) -> Result<Drained<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>, Self> {
        if matches!(self.output.pending(&self.slot), egress::Pending::Present) {
            return Err(self);
        }
        Ok(Drained {
            control: self.slot.source,
            state: self.slot.conn,
            output: marker::PhantomData,
        })
    }

    /// Reborrows the exact application-turn capability carried by this
    /// callback context.
    pub fn work(&self) -> app::Work<'a, 'd> {
        self.work
    }

    /// Returns a driver- and generation-branded target for deferred wakeups.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.slot.io().wake_target()
    }

    pub fn write(&mut self) -> write::Context<'e, '_, 'a, 'p, 's, 'w, 'd, ID, W, C, B> {
        write::Context::new(self)
    }

    /// Whether this connection already carries a transport close-after fence.
    pub fn is_closing(&self) -> bool {
        self.slot.is_draining()
    }
}

const _: () = assert!(mem::size_of::<Id<'static, KeyTag<0>>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Id<'static, KeyTag<0>>>() == mem::align_of::<route::Token>());
const _: () = assert!(
    mem::size_of::<Retired<'static, 'static, 'static, KeyTag<0>, wire::Identity, ()>>()
        == mem::size_of::<Id<'static, KeyTag<0>>>()
            + mem::size_of::<Option<Failure>>()
            + mem::size_of::<Option<Transmit>>()
);
