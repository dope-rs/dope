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
        writer::{deadline::Slot, flow, flow::SlotFlow as _, memory},
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
    Park(event::ParkError),
    Write(body::Failure),
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

mod state;

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
    B = resident::Shared<'d>,
> {
    slot: &'a types::Connection<'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
}

impl<'a, 'p, 's, 'w, 'd: 'w, ID: Identity, W: wire::Spec + 'w + 'd, C, B>
    Ref<'a, 'p, 's, 'w, 'd, ID, W, C, B>
{
    pub(super) const fn new(
        slot: &'a types::Connection<'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
    ) -> Self {
        Self { slot }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(self.slot.key())
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }

    /// Returns whether the wire has finished terminal convergence.
    /// Application work may still require closure to remain deferred.
    pub fn is_terminal(&self) -> bool {
        self.slot.terminal.is_done()
    }
}

/// Final owned state after the connection slot and every egress owner retire.
pub struct Retired<'p, 's, 'd, ID: Identity, W: wire::Spec, C, B = resident::Shared<'d>> {
    id: Id<'d, ID>,
    state: State<'p, 's, 'd, ID, C, B>,
    transmit: Option<Transmit>,
    wire: marker::PhantomData<W>,
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
    slot: Slot<'a, 'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
    output: Output<'a, 'p, 's, 'e, 'd, ID, B>,
}

pub(super) enum Drain<T> {
    Ready(T),
    Pending,
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
    pub fn id(&self) -> Id<'d, ID> {
        Id(self.slot.key())
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.slot.state.conn
    }

    /// Records a close intent without acquiring output or scheduling
    /// authority. The listener observes it after the drain callback returns.
    pub fn close_after(&mut self) {
        self.slot.close_after();
    }
}

impl<'p, 's, 'd, ID: Identity, W: wire::Spec, C, B> Retired<'p, 's, 'd, ID, W, C, B> {
    pub(in crate::network::listener) fn take(
        key: pool::Key<'d, ID>,
        mut state: State<'p, 's, 'd, ID, C, B>,
        kind: close::Kind,
    ) -> (Option<B>, Option<Self>) {
        let (egress, transmit) = state.send.retire();
        let conn = match kind {
            close::Kind::Connected => Some(Self {
                id: Id::from_key(key),
                state,
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
        &self.state.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.state.conn
    }

    /// Exact listener lifecycle failure retained until the final owner audit.
    pub const fn failure(&self) -> Option<&Failure> {
        self.state.failure.as_ref()
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
    pub(super) slot: Slot<'a, 'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
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
    fn pending<'w, W: wire::Spec + 'w + 'd, C>(
        &self,
        slot: &types::Connection<'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
    ) -> egress::Pending
    where
        'd: 'w,
    {
        let pending = match self {
            Self::Open { queue, .. } => queue.pending(),
            Self::Sealed => egress::Pending::Absent,
        };
        if slot.send_status().is_closing() || !matches!(slot.flow(pending), flow::Flow::Clear) {
            egress::Pending::Present
        } else {
            egress::Pending::Absent
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
    pub(super) fn sealed(
        slot: &'a mut types::Connection<'w, 'd, ID, W, State<'p, 's, 'd, ID, C, B>>,
        work: app::Work<'a, 'd>,
    ) -> Self {
        Self {
            slot: Slot::closed(slot),
            output: Output::Sealed,
            work,
        }
    }

    pub fn id(&self) -> Id<'d, ID> {
        Id(self.slot.key())
    }

    pub fn state(&self) -> &C {
        &self.slot.state.conn
    }

    pub fn state_mut(&mut self) -> &mut C {
        &mut self.slot.state.conn
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
    pub(super) fn drain(self) -> Drain<Drained<'e, 'a, 'p, 's, 'w, 'd, ID, W, C, B>> {
        let scope = Drained {
            slot: self.slot,
            output: self.output,
        };
        match scope.output.pending(&scope.slot) {
            egress::Pending::Present => Drain::Pending,
            egress::Pending::Absent => Drain::Ready(scope),
        }
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
            + mem::size_of::<State<'static, 'static, 'static, KeyTag<0>, ()>>()
            + mem::size_of::<Option<Transmit>>()
);
