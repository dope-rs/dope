use std::{convert::Infallible, marker::PhantomData};

use dope_core::{
    driver::{
        self,
        route::{Identity, KeyTag},
        schedule::{self, app::Permit, clock},
    },
    io::socket,
};
use dope_net::{
    link::{
        egress::{self, data},
        event,
        pool::{change::opening, recv::input},
    },
    wire::{domain, open, receive::Chunk, terminal},
};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer::bytes,
    mem::rc,
};

use crate::{
    app::{
        Park,
        receive::{self, limits},
    },
    client::{
        self, attempt, close,
        conn::{self, drain},
    },
};

pub mod admit;
pub mod defer;
pub mod failure;
mod policy;
mod receiver;
pub mod request;
pub mod resume;

pub(in crate::client) use policy::Policy;
pub(in crate::client) use receiver::Receiver;
pub mod connect;

pub trait Borrowed<'d, ID: Identity = KeyTag<0>>:
    Receive<'d, ID, Input = receive::Borrowed, Continuation: resume::Mode<Retention = resume::Absent>>
{
    fn chunk<'e, O, R: bytes::TryRetain>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome;
}

pub trait Owned<'w, 'd: 'w, ID: Identity = KeyTag<0>>:
    Receive<'d, ID, Input = receive::Owned>
{
    const RETENTION: limits::Retention;

    /// Supplies this instance's receive-credit bound once during installation.
    fn retention(&self) -> limits::Retention {
        Self::RETENTION
    }

    fn chunk<'e, O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        chunk: Chunk<'w, 'd, Self::Wire>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> <Self::Continuation as resume::Mode>::Outcome;
}

/// Exact typed completion result for a delivered receive chunk.
#[must_use = "the receive result must be applied to the connection"]
#[repr(transparent)]
pub struct ChunkOutcome(Result<(), receive::Failure>);

impl ChunkOutcome {
    pub const fn ok() -> Self {
        Self(Ok(()))
    }

    pub const fn failed(cause: receive::Failure) -> Self {
        Self(Err(cause))
    }

    pub const fn into_result(self) -> Result<(), receive::Failure> {
        self.0
    }
}

const _: () = assert!(size_of::<ChunkOutcome>() == size_of::<receive::Failure>());

pub enum Resumed {
    Complete(ChunkOutcome),
    Paused(Pause),
}

/// Scheduling result for incomplete resumable receive work.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Pause {
    Yield,
    Blocked,
}

const _: () = assert!(size_of::<Resumed>() == size_of::<ChunkOutcome>());

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Wake {
    Idle,
    Ready,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Eof {
    Close,
    Retain,
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Fence {
    Ready,
    Pending,
}

const _: () = {
    assert!(size_of::<Wake>() == 1);
    assert!(size_of::<Eof>() == 1);
    assert!(size_of::<Fence>() == 1);
};

pub enum CloseOutcome {
    Complete(close::Directive),
    Yield,
}

/// Application admission for one transport-connected primary generation.
/// Rejection retires only transport ownership; no application retirement
/// callback is emitted for a generation that was never attached.
#[must_use = "connection admission must be committed or rejected"]
pub enum Admission {
    /// Application ownership is live until one matching `drained`.
    Attached,
    /// No application ownership remains; retire the physical transport only.
    Rejected(close::Directive),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Inbound {
    /// No protocol response is currently expected, so inbound silence is safe.
    Quiescent,
    /// A response is outstanding and must make inbound progress within this window.
    Awaiting(Positive),
}

pub trait Application<'d, ID: Identity = KeyTag<0>>: Sized {
    type Conn;
    type Connect: connect::Mode;
    type Deferred: defer::Mode;
    type Failure: failure::Mode;
    type Wire: input::Execution + 'd;
    type Send: data::raw::Payload<'d>;
    type Input: receive::Mode<Self::Wire>;
    type Requests: request::Mode;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error>;
}

pub trait Receive<'d, ID: Identity = KeyTag<0>>: Application<'d, ID> {
    type Continuation: resume::Mode;

    /// Classifies application-owned retained state for connection retirement.
    fn quiesce<'a>(
        &self,
        barrier: Barrier<'a, 'd, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Drain<'a, 'd, ID>;

    /// Observes orderly input end and whether the wire also ended output.
    fn eof<'e, O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _: receive::End,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        _: &mut driver::Context<'_, 'd>,
    ) -> Eof {
        Eof::Close
    }

    fn fence<O>(
        &self,
        _: conn::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        _: &mut driver::Context<'_, 'd>,
    ) -> Fence {
        Fence::Ready
    }
}

pub trait Resumable<'d, ID: Identity = KeyTag<0>>: Receive<'d, ID> {
    /// Returns the resident capacity owned by retained receive storage.
    fn resident(&self) -> domain::Resident;

    /// Consumes an application-owned blocked marker for this connection.
    /// `Ready` asks the engine to retry its exact retained receive.
    fn wake(&mut self, conn: &mut Self::Conn) -> Wake;

    /// Describes whether this exact connection is waiting for inbound progress.
    fn inbound<O>(
        &self,
        conn: conn::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        base: Positive,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Inbound;

    fn resume<'e, 'turn, O>(
        &mut self,
        permit: Permit<'turn, 'd>,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> <Self::Continuation as resume::Mode>::Outcome;
}

/// Move-only authority to classify one exact connection's application drain.
#[must_use = "drain authority must produce an exact outcome"]
#[repr(transparent)]
pub struct Barrier<'a, 'd, ID: Identity>(
    &'a drain::Drain<'d, ID>,
    PhantomData<fn(&'a ()) -> &'a ()>,
);

impl<'a, 'd, ID: Identity> Barrier<'a, 'd, ID> {
    pub(in crate::client) const fn new(drain: &'a drain::Drain<'d, ID>) -> Self {
        Self(drain, PhantomData)
    }

    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.0.connection()
    }

    pub const fn pending(self) -> Drain<'a, 'd, ID> {
        Drain(Outcome::Pending(self))
    }

    pub const fn ready(self) -> Drain<'a, 'd, ID> {
        Drain(Outcome::Ready(self))
    }
}

/// Exact application-drain result for one connection generation.
#[must_use = "drain outcome must reach connection retirement"]
#[repr(transparent)]
pub struct Drain<'a, 'd, ID: Identity>(Outcome<'a, 'd, ID>);

enum Outcome<'a, 'd, ID: Identity> {
    Pending(Barrier<'a, 'd, ID>),
    Ready(Barrier<'a, 'd, ID>),
}

pub(in crate::client) enum Resolution<'d, ID: Identity> {
    Pending,
    Ready(Quiesced<'d, ID>),
}

impl<'a, 'd, ID: Identity> Drain<'a, 'd, ID> {
    pub(in crate::client) const fn resolve(self) -> Resolution<'d, ID> {
        match self.0 {
            Outcome::Pending(Barrier(..)) => Resolution::Pending,
            Outcome::Ready(barrier) => Resolution::Ready(Quiesced(barrier.connection())),
        }
    }
}

/// Move-only proof that one exact connection's application state is quiescent.
#[must_use = "quiescence proof must reach connection retirement"]
#[repr(transparent)]
pub struct Quiesced<'d, ID: Identity>(conn::Id<'d, ID>);

impl<'d, ID: Identity> Quiesced<'d, ID> {
    pub const fn connection(&self) -> conn::Id<'d, ID> {
        self.0
    }
}

const _: () = {
    assert!(
        size_of::<Barrier<'static, 'static, KeyTag<0>>>()
            == size_of::<&conn::Id<'static, KeyTag<0>>>()
    );
    assert!(size_of::<Quiesced<'static, KeyTag<0>>>() == size_of::<conn::Id<'static, KeyTag<0>>>());
    assert!(size_of::<Drain<'static, 'static, KeyTag<0>>>() == 2 * size_of::<usize>());
    assert!(
        std::mem::align_of::<Drain<'static, 'static, KeyTag<0>>>() == std::mem::align_of::<usize>()
    );
};

pub trait Lifecycle<'w, 'd: 'w, ID: Identity = KeyTag<0>>: Receive<'d, ID> {
    fn prepare<'e, O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn sent(
        &mut self,
        conn: conn::Id<'d, ID>,
        pending: egress::Pending,
        token: &mut region::Token<region::Identity<'d, 1>>,
    );

    fn cancel(&mut self, payload: Self::Send, token: &mut region::Token<region::Identity<'d, 1>>);

    /// Observes the exact owned wire receive failure before transport teardown.
    fn wire_fail<'e, O>(
        &mut self,
        conn: conn::Io<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        cause: terminal::Error<'w, Self::Wire>,
        driver: &mut driver::Context<'_, 'd>,
    );

    /// Observes the exact transport receive failure before teardown.
    fn io_fail<'e, O>(
        &mut self,
        conn: conn::Io<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        error: dope_core::Errno,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn close<'e, O>(
        &mut self,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        reason: close::Reason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> CloseOutcome;

    /// Finalizes application state only after transport send/receive ownership
    /// and the exact connection's egress queue have been retired.
    fn drained(
        &mut self,
        proof: conn::Retired<'d, ID, Self>,
        state: &mut Self::Conn,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Deferred<'d, ID: Identity = KeyTag<0>>:
    Application<'d, ID, Deferred = defer::Observed>
{
    fn deferred(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: opening::Deferred,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Opened<'w, 'd: 'w, ID: Identity = KeyTag<0>>: Application<'d, ID> {
    fn open(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: open::Error<'w, Self::Wire>,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Connected<'d, ID: Identity = KeyTag<0>>:
    Application<'d, ID, Connect = connect::Full>
{
    type Activation: admit::Dispatch<'d, ID, Self>;

    fn connected<'e, O>(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        peer: socket::Addr,
        conn: conn::Established<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Peer<'d, ID: Identity = KeyTag<0>>: Application<'d, ID, Connect = connect::Peer> {
    type Addr;

    fn connected<'e, O>(
        &mut self,
        peer: Self::Addr,
        conn: conn::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Exhausted<'d, ID: Identity = KeyTag<0>>: Application<'d, ID> {
    fn exhaust(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Failed<'d, ID: Identity = KeyTag<0>>:
    Exhausted<'d, ID> + Application<'d, ID, Failure = failure::Observed>
{
    fn failed(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: event::ConnectFault,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Requests<'d, ID: Identity = KeyTag<0>>:
    Application<'d, ID, Requests = request::Enabled>
{
    fn drain<'e>(&self, turn: &mut request::Turn<'_, 'e, 'd, Self::Conn, Self::Send, ID>);
}

pub trait Scheduling<'d, ID: Identity = KeyTag<0>>: Application<'d, ID> {
    fn park() -> Option<impl for<'a, 'b, 'c, 'e> Call<(&'a mut Self, &'b mut Park<'c, 'e, 'd>)>> {
        None::<Infallible>
    }

    /// Starts application-owned retained-state retirement. The engine calls
    /// this exactly once before it begins closing connection slots.
    fn shutdown() -> Option<impl for<'a> Call<&'a mut Self>> {
        None::<Infallible>
    }

    fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
        clock: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d>;
}
