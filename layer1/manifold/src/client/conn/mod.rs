use dope_core::driver::{
    result,
    route::{Identity, KeyTag, indices},
};
pub(in crate::client) mod claim;
pub(in crate::client) mod closing;
pub(in crate::client) mod drain;
pub mod engine;
pub(in crate::client) mod flow;
mod lane;
mod lifetime;
mod state;

use std::{marker, mem};

use claim::Urgency;
use closing::{Closing, retire};
use dope_core::driver::{
    route,
    schedule::{app::Work, clock, ready},
};
use dope_net::{
    link::{
        event, pool,
        slot::{transmit, types},
    },
    wire,
};
pub use lane::Lane;
pub use lifetime::Lifetime;
use o3::{
    base::num::bounded,
    depot::{self, index::Initialized},
};

use crate::{
    app::dispatch::identity,
    client::{
        app::Quiesced,
        attempt, close,
        session::{self, app},
    },
    service::bindings::{Bound, endpoint},
};

/// The native slot retains the exact application state and connection ownership.
pub(in crate::client) type Slot<'w, 'd, ID, W, C, O> =
    types::Connection<'w, 'd, ID, W, State<'d, C, O>>;

/// A live connection and its retained owner remain tied to their exact key.
pub(in crate::client) type Source<O, C, K> = depot::raw::Proof<((O, C), K), Initialized<()>>;

/// Driver- and route-branded connector identity.
///
/// ```compile_fail
/// use dope_core::driver::route::KeyTag;
/// use dope_manifold::client::conn::Id;
///
/// fn widen<'short, 'long>(id: Id<'short, KeyTag<1>>) -> Id<'long, KeyTag<1>>
/// where
///     'long: 'short,
/// {
///     id
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Id<'d, ID: Identity>(pool::Key<'d, ID>);

impl<'d, ID: Identity> Id<'d, ID> {
    pub(in crate::client) fn from_key(key: pool::Key<'d, ID>) -> Self {
        Self(key)
    }

    pub const fn index(self) -> bounded::U32<0, { route::SLOT_MASK as u32 }> {
        self.0.index()
    }

    /// Projects this exact generation into its generation-independent lane.
    pub const fn lane(self) -> Lane<'d, ID> {
        Lane::new(self.0.lane())
    }

    pub(in crate::client) const fn key(self) -> pool::Key<'d, ID> {
        self.0
    }
}

impl<ID: Identity> identity::Identity for Id<'_, ID> {
    fn slot(self) -> indices::SlotIndex {
        self.0.lane()
    }
}

/// Move-only pre-connect proof. Exact-generation resources use
/// [`Self::connection`], while generation-free resources use [`Self::lane`].
#[must_use = "connection activation must be admitted or rejected"]
pub struct Activation<'a, 'd, ID: Identity> {
    attempt: &'a attempt::Completion<'a, 'd, ID>,
    connection: Id<'d, ID>,
    ready: ready::Target<'d>,
}

impl<'a, 'd, ID: Identity> Activation<'a, 'd, ID> {
    pub(in crate::client) const fn new(
        attempt: &'a attempt::Completion<'a, 'd, ID>,
        conn: Id<'d, ID>,
        ready: ready::Target<'d>,
    ) -> Self {
        Self {
            attempt,
            connection: conn,
            ready,
        }
    }

    /// Returns the exact connection attempt being activated.
    pub const fn attempt(&self) -> attempt::Id<'d, ID> {
        self.attempt.id()
    }

    pub const fn completion(&self) -> &attempt::Completion<'a, 'd, ID> {
        self.attempt
    }

    /// Returns the exact generation being activated.
    pub const fn connection(&self) -> Id<'d, ID> {
        self.connection
    }

    /// Returns the generation-independent physical lane.
    pub const fn lane(&self) -> Lane<'d, ID> {
        self.connection.lane()
    }

    /// Returns the driver-owned wake target for this activation.
    pub const fn ready(&self) -> ready::Target<'d> {
        self.ready
    }
}

/// Move-only proof that one exact connection generation and application drained.
/// Its invariant application type is independent of replaceable connection state.
#[must_use = "retired connection proof owns the terminal close cause"]
pub struct Retired<'d, ID: Identity, A> {
    app: Quiesced<'d, ID>,
    reason: close::Reason,
    transmit: Option<Transmit>,
    source: marker::PhantomData<fn(A) -> A>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Transmit {
    Io(event::SendFailure<dope_core::Errno>),
    Rejected(event::SendFailure<result::SubmitError>),
    Invalid(transmit::Invalid),
}

impl<'d, ID: Identity, A> Retired<'d, ID, A> {
    const fn new(app: Quiesced<'d, ID>, reason: close::Reason, transmit: Option<Transmit>) -> Self {
        Self {
            app,
            reason,
            transmit,
            source: marker::PhantomData,
        }
    }

    pub const fn connection(&self) -> Id<'d, ID> {
        self.app.connection()
    }

    pub const fn reason(&self) -> close::Reason {
        self.reason
    }

    pub const fn transmit(&self) -> Option<&Transmit> {
        self.transmit.as_ref()
    }
}

impl<'w, 'd: 'w, ID: Identity, N: session::Parser<'d, ID>, W: wire::Spec + 'd>
    Retired<'d, ID, app::State<'w, 'd, ID, N, W>>
{
    pub(in crate::client) fn into_session(self) -> Retired<'d, ID, N> {
        Retired {
            app: self.app,
            reason: self.reason,
            transmit: self.transmit,
            source: marker::PhantomData,
        }
    }
}

impl<'life, 'd: 'life, P: ?Sized, L, F, ID: Identity, const MAX: usize>
    Retired<'d, ID, Bound<'life, 'd, P, L, ID, MAX, F>>
{
    pub(crate) fn into_policy(self) -> Retired<'d, ID, endpoint::State<'life, 'd, P, L, ID, MAX>> {
        Retired {
            app: self.app,
            reason: self.reason,
            transmit: self.transmit,
            source: marker::PhantomData,
        }
    }
}

pub(crate) use state::State;

/// Callback-scoped proof to mutate exactly one branded connection's close
/// state. During retirement it carries a non-empty packed-state proof.
pub struct CloseRequest<'a, 'd, ID: Identity> {
    target: CloseTarget<'a>,
    brand: marker::PhantomData<fn(Id<'d, ID>) -> Id<'d, ID>>,
}

enum CloseTarget<'a> {
    Open(&'a mut Closing),
    Retiring(closing::RetireRef<'a>),
}

#[must_use = "close request must distinguish an applied and sealed directive"]
pub enum CloseUpdate {
    Applied(close::Directive),
    Sealed(close::Directive),
}

/// Exact progress of one connection's close machine.
///
/// The machine only exists once a close request has sealed its generation, so
/// [`CloseStatus::Open`] is expressed by the absence of any phase.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ClosePhase {
    Requested,
    Quarantining,
    Retiring,
    Settled,
    Finalized,
    Claimed,
}

/// Observable close progress of one connection generation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CloseStatus {
    Open,
    Closing(ClosePhase, close::Directive),
}

impl<'a, 'd, ID: Identity> CloseRequest<'a, 'd, ID> {
    fn new(closing: &'a mut Closing) -> Self {
        Self {
            target: CloseTarget::Open(closing),
            brand: marker::PhantomData,
        }
    }

    pub(super) fn reborrow(&mut self) -> CloseRequest<'_, 'd, ID> {
        match &mut self.target {
            CloseTarget::Open(closing) => CloseRequest::new(closing),
            CloseTarget::Retiring(state) => CloseRequest {
                target: CloseTarget::Retiring(state.reborrow()),
                brand: marker::PhantomData,
            },
        }
    }

    pub fn request(&mut self, rule: close::Directive) {
        match &mut self.target {
            CloseTarget::Open(closing) => {
                closing.apply(rule);
            }
            CloseTarget::Retiring(state) => state.request(rule),
        }
    }

    pub const fn status(&self) -> CloseStatus {
        match &self.target {
            CloseTarget::Open(closing) => closing.status(),
            CloseTarget::Retiring(state) => state.status(),
        }
    }
}

#[repr(transparent)]
pub struct Ref<'a, 'd, ID: Identity, W: wire::Spec, C, O = ()> {
    active: state::ActiveRef<'a, 'd, ID, W, C, O>,
}

impl<'a, 'd, ID: Identity, W: wire::Spec, C, O> Ref<'a, 'd, ID, W, C, O> {
    pub(super) const fn new(active: state::ActiveRef<'a, 'd, ID, W, C, O>) -> Self {
        Self { active }
    }

    pub fn id(&self) -> Id<'d, ID> {
        self.active.id()
    }

    pub(in crate::client) const fn last_recv(&self) -> clock::Moment<'d> {
        self.active.last_recv()
    }

    pub fn state(&self) -> &C {
        self.active.conn()
    }
}

#[must_use = "established connection context owns the activation turn"]
pub struct Established<'a, 'd, ID: Identity, W: wire::Spec, C, O = ()> {
    active: state::ActiveMut<'a, 'd, ID, W, C, O>,
    expiry: Option<clock::Deadline<'d>>,
}

impl<'a, 'd, ID: Identity, W: wire::Spec, C, O> Established<'a, 'd, ID, W, C, O> {
    pub(super) const fn new(
        active: state::ActiveMut<'a, 'd, ID, W, C, O>,
        expiry: Option<clock::Deadline<'d>>,
    ) -> Self {
        Self { active, expiry }
    }

    pub fn id(&self) -> Id<'d, ID> {
        self.active.id()
    }

    pub fn wake_target(&self) -> ready::Target<'d> {
        self.active.ready()
    }

    pub(super) fn into_io(
        self,
    ) -> (
        Lifetime<'d, ID>,
        &'a mut C,
        CloseRequest<'a, 'd, ID>,
        ready::Target<'d>,
    ) {
        let lifetime = Lifetime::new(self.active.id(), self.expiry);
        let ready = self.active.ready();
        let (conn, closing) = self.active.into_parts();
        (lifetime, conn, CloseRequest::new(closing), ready)
    }
}

pub struct Ctx<'a, 'd, ID: Identity, W: wire::Spec, C, O = ()> {
    active: state::ActiveMut<'a, 'd, ID, W, C, O>,
    work: Work<'a, 'd>,
}

/// Borrows the exact connection and close request for an I/O failure callback.
pub struct Io<'a, 'd, ID: Identity, W: wire::Spec, C, O = ()> {
    id: Id<'d, ID>,
    conn: &'a mut C,
    closing: CloseRequest<'a, 'd, ID>,
    ready: ready::Target<'d>,
    spec: marker::PhantomData<fn(W, O)>,
}

impl<'a, 'd, ID: Identity, W: wire::Spec, C, O> Io<'a, 'd, ID, W, C, O> {
    pub const fn id(&self) -> Id<'d, ID> {
        self.id
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Id<'d, ID>,
        &'a mut C,
        CloseRequest<'a, 'd, ID>,
        ready::Target<'d>,
    ) {
        (self.id, self.conn, self.closing, self.ready)
    }
}

impl<'a, 'd, ID: Identity, W: wire::Spec, C, O> Ctx<'a, 'd, ID, W, C, O> {
    pub(super) const fn new(
        active: state::ActiveMut<'a, 'd, ID, W, C, O>,
        work: Work<'a, 'd>,
    ) -> Self {
        Self { active, work }
    }

    pub fn id(&self) -> Id<'d, ID> {
        self.active.id()
    }

    pub fn state(&self) -> &C {
        self.active.conn()
    }

    pub fn state_mut(&mut self) -> &mut C {
        self.active.conn_mut()
    }

    /// Borrows the first exact send failure retained by this connection.
    pub const fn transmit(&self) -> Option<&Transmit> {
        self.active.transmit()
    }

    /// Latches one indivisible close decision into this exact connection.
    pub fn request(&mut self, rule: close::Directive) {
        self.active.closing_mut().apply(rule);
    }

    /// Returns the exact application-turn capability carried by this callback.
    pub fn work(&self) -> Work<'a, 'd> {
        self.work
    }

    /// Returns the exact driver- and generation-branded readiness target.
    pub fn wake_target(&self) -> ready::Target<'d> {
        self.active.ready()
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Id<'d, ID>,
        &'a mut C,
        CloseRequest<'a, 'd, ID>,
        Work<'a, 'd>,
        ready::Target<'d>,
    ) {
        let id = self.active.id();
        let ready = self.active.ready();
        let (conn, closing) = self.active.into_parts();
        (id, conn, CloseRequest::new(closing), self.work, ready)
    }

    pub(super) fn into_retire(
        self,
    ) -> (
        Id<'d, ID>,
        &'a mut C,
        retire::Retire<'a, 'd, ID>,
        Work<'a, 'd>,
        ready::Target<'d>,
    ) {
        let id = self.active.id();
        let ready = self.active.ready();
        let (conn, closing, proof) = self.active.into_retire();
        (
            id,
            conn,
            retire::Retire::new(closing, proof),
            self.work,
            ready,
        )
    }

    pub(super) fn into_io(
        self,
    ) -> (
        Id<'d, ID>,
        &'a mut C,
        CloseRequest<'a, 'd, ID>,
        ready::Target<'d>,
    ) {
        let (id, conn, close, _, ready) = self.into_parts();
        (id, conn, close, ready)
    }
}

const _: () = assert!(mem::size_of::<Id<'static, KeyTag<0>>>() == mem::size_of::<route::Token>());
const _: () = assert!(mem::align_of::<Id<'static, KeyTag<0>>>() == mem::align_of::<route::Token>());
const _: () = assert!(
    mem::size_of::<Ctx<'static, 'static, KeyTag<0>, wire::Identity, ()>>()
        == 7 * mem::size_of::<usize>()
);
const _: () = {
    assert!(
        mem::size_of::<Io<'static, 'static, KeyTag<0>, wire::Identity, ()>>()
            == 7 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Io<'static, 'static, KeyTag<0>, wire::Identity, ()>>>()
            == 7 * mem::size_of::<usize>()
    );
};
const _: () = assert!(
    mem::size_of::<Established<'static, 'static, KeyTag<0>, wire::Identity, ()>>()
        + mem::size_of::<Id<'static, KeyTag<0>>>()
        + mem::size_of::<Work<'static, 'static>>()
        == mem::size_of::<Ctx<'static, 'static, KeyTag<0>, wire::Identity, ()>>()
            + mem::size_of::<Lifetime<'static, KeyTag<0>>>()
);
const _: () = assert!(
    mem::size_of::<Ref<'static, 'static, KeyTag<0>, wire::Identity, ()>>()
        == 3 * mem::size_of::<usize>()
);
