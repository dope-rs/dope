use dope_core::{
    driver::{self, schedule},
    io::socket,
};
use dope_net::{
    link::{
        egress::{self, data, metadata},
        event,
        pool::transition::open,
    },
    wire,
};
use o3::{buffer::bytes, cell::region};

use crate::{
    connector::{self, attempt, connection, lifecycle},
    receive, timing,
};

pub mod continuation;
mod policy;
mod receiver;

pub(crate) use policy::Policy;
pub(crate) use receiver::Receiver;

pub trait Mode<'d, const ID: u8, A>: receive::Mode<A::Wire> + Policy<'d, ID, A>
where
    A: Application<'d, ID>,
{
}

pub trait BorrowedReceive<'d, const ID: u8 = 0>:
    Receive<'d, ID, Input = receive::Borrowed>
{
    fn chunk<O, R: bytes::Retainable>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> <Self::Continuation as continuation::Mode<'d, ID, Self>>::Outcome;
}

pub trait RetainedReceive<'d, const ID: u8 = 0>:
    Receive<'d, ID, Input = receive::Retained>
{
    const RETENTION: receive::Retention;

    fn retained_chunk<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        chunk: <Self::Wire as wire::Wire>::RetainedRecv<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> <Self::Continuation as continuation::Mode<'d, ID, Self>>::Outcome;
}

impl<'d, const ID: u8, A> Mode<'d, ID, A> for receive::Borrowed where A: BorrowedReceive<'d, ID> {}

impl<'d, const ID: u8, A> Mode<'d, ID, A> for receive::Retained where A: RetainedReceive<'d, ID> {}

type OpenDeferred = open::Deferred;
type OpenFailure<E> = open::Failure<E>;

pub enum ChunkOutcome {
    Ok,
    Capacity,
    Overrun,
    CloseReconnect,
    ClosePermanent,
}

pub enum ResumeOutcome {
    Complete(ChunkOutcome),
    Yield,
}

pub enum CloseOutcome {
    Complete(lifecycle::CloseReason),
    Yield,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Inbound {
    /// No protocol response is currently expected, so inbound silence is safe.
    Quiescent,
    /// A response is outstanding and must make inbound progress within this window.
    Awaiting(timing::Window),
}

pub enum OpenOutcome<E> {
    Failed(OpenFailure<E>),
    Deferred(OpenDeferred),
}

/// How [`Application::drain_requests`] treats the dial target on close.
/// This gives requests outside the receive path both recoverable and terminal
/// outcomes, matching [`ChunkOutcome`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseKind {
    /// Drop the socket and redial (transient: idle timeout, rotation).
    Reconnect,
    /// Drop the socket and DO NOT redial (terminal: retries exhausted,
    /// credentials permanently rejected).
    Permanent,
}

#[derive(Default)]
pub struct Requests {
    pub close: Option<CloseKind>,
}

/// Turn-bounded ownership transfer into one connection's egress queue.
/// A permit spends one work unit and transfers at most one payload; neither
/// the queue borrow nor driver lifetime can escape `drain_requests`.
pub struct RequestDrain<'queue, 'd, B: data::Payload<'d>> {
    queue: egress::Queue<'queue, 'd, { connector::IOV_CAP }, B>,
    work: schedule::Application<'queue, 'd>,
    enqueued: bool,
    exhausted: bool,
}

#[must_use]
pub struct RequestPermit<'permit, 'queue, 'd, B: data::Payload<'d>> {
    drain: &'permit mut RequestDrain<'queue, 'd, B>,
}

/// A located request which leaves its source unchanged until admission.
pub trait RequestFront {
    type Item;

    fn take(self) -> Self::Item;
}

impl<'queue, 'token, 'd, T> RequestFront for metadata::FrontEntry<'queue, 'token, 'd, T> {
    type Item = (T, metadata::Front<'queue, 'token, 'd, T>);

    fn take(self) -> Self::Item {
        self.take()
    }
}

#[must_use = "request admission determines whether the located source was consumed"]
pub enum RequestAdmission<'permit, 'queue, 'd, B: data::Payload<'d>, T> {
    Item(T, RequestPermit<'permit, 'queue, 'd, B>),
    Empty,
    Exhausted,
}

impl<'queue, 'd, B: data::Payload<'d>> RequestDrain<'queue, 'd, B> {
    pub(super) fn new(
        queue: egress::Queue<'queue, 'd, { connector::IOV_CAP }, B>,
        work: schedule::Application<'queue, 'd>,
    ) -> Self {
        Self {
            queue,
            work,
            enqueued: false,
            exhausted: false,
        }
    }

    /// Admits an already-located source front without consuming it on yield.
    pub fn admit<F>(&mut self, front: Option<F>) -> RequestAdmission<'_, 'queue, 'd, B, F::Item>
    where
        F: RequestFront,
    {
        let Some(front) = front else {
            return RequestAdmission::Empty;
        };
        if !self.work.take() {
            self.exhausted = true;
            return RequestAdmission::Exhausted;
        }
        let item = front.take();
        RequestAdmission::Item(item, RequestPermit { drain: self })
    }

    pub fn admit_with<F, T>(
        &mut self,
        front: Option<F>,
        acquire: impl FnOnce(F) -> Option<T>,
    ) -> RequestAdmission<'_, 'queue, 'd, B, T> {
        let Some(front) = front else {
            return RequestAdmission::Empty;
        };
        match self.work.admit_with(|| acquire(front)) {
            schedule::Admission::Item(item) => {
                RequestAdmission::Item(item, RequestPermit { drain: self })
            }
            schedule::Admission::Empty => RequestAdmission::Empty,
            schedule::Admission::Exhausted => {
                self.exhausted = true;
                RequestAdmission::Exhausted
            }
        }
    }

    pub(in crate::connector) const fn enqueued(&self) -> bool {
        self.enqueued
    }

    pub(in crate::connector) const fn exhausted(&self) -> bool {
        self.exhausted
    }
}

impl<'queue, 'd, B: data::Payload<'d>> RequestPermit<'_, 'queue, 'd, B> {
    /// Consumes this permit while preserving payload ownership on egress
    /// saturation.
    pub fn try_push(self, region: &mut region::Token<'d>, value: B) -> Result<(), B> {
        self.drain.queue.try_enqueue(region, value)?;
        self.drain.enqueued = true;
        Ok(())
    }
}

pub trait Application<'d, const ID: u8 = 0>: Sized {
    type Conn;
    type Wire: wire::Wire;
    type Send: data::Payload<'d>;
    type Input: Mode<'d, ID, Self>;

    fn connection(&self) -> Self::Conn;
}

pub trait Receive<'d, const ID: u8 = 0>: Application<'d, ID> {
    type Continuation: continuation::Mode<'d, ID, Self>;
}

pub trait ResumableReceive<'d, const ID: u8 = 0>:
    Receive<'d, ID, Continuation = continuation::Resumable>
{
    fn resume<'turn, O>(
        &mut self,
        permit: schedule::ApplicationPermit<'turn, 'd>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> ResumeOutcome;
}

pub trait Lifecycle<'d, const ID: u8 = 0>: Application<'d, ID> {
    fn connected<O>(
        &mut self,
        key: attempt::Id<'d, ID>,
        peer: socket::Addr,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    );

    fn connect_failed(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: event::ConnectFailure,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let _ = (key, cause, driver);
    }

    fn open(
        &mut self,
        key: attempt::Id<'d, ID>,
        outcome: OpenOutcome<<Self::Wire as wire::Wire>::OpenError>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let _ = (key, outcome, driver);
    }

    fn before_send<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let _ = (connection, egress, driver);
    }

    fn sent(&mut self, connection: connection::Id<'d, ID>, has_pending_egress: bool);

    fn close<O>(
        &mut self,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        egress: egress::Queue<'_, 'd, { connector::IOV_CAP }, Self::Send>,
        reason: lifecycle::CloseReason,
        driver: &mut driver::Context<'_, 'd>,
    ) -> CloseOutcome;

    /// Returns the close intent sampled after each state-mutating callback,
    /// including the final close callback.
    fn close_kind<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Option<CloseKind> {
        let _ = (connection, driver);
        None
    }

    fn defer_close<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        let _ = (connection, driver);
        false
    }

    fn is_drained<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> bool {
        let _ = (connection, driver);
        true
    }

    /// Describes whether this exact connection is waiting for inbound progress.
    /// The returned borrow-free state cannot escape the current driver turn.
    fn inbound<O>(
        &self,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn, O>,
        default: timing::Window,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Inbound {
        let _ = (connection, default, driver);
        Inbound::Quiescent
    }
}

pub trait RequestSource<'d, const ID: u8 = 0>: Application<'d, ID> {
    fn drain_requests(
        &self,
        connection: connection::Id<'d, ID>,
        state: &mut Self::Conn,
        drain: &mut RequestDrain<'_, 'd, Self::Send>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Requests;
}

pub trait Scheduling<'d, const ID: u8 = 0>: Application<'d, ID> {
    fn pre_park<'turn>(
        &mut self,
        work: schedule::Application<'turn, 'd>,
        region: &mut region::Token<'d>,
    );

    /// Starts application-owned retained-state retirement. The engine calls
    /// this exactly once before it begins closing connection slots.
    fn shutdown(&mut self);

    fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d>;
}
