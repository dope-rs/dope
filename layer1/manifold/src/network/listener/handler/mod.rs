use std::{pin, time};

use dope_core::{
    driver::{
        retained,
        route::{Identity, KeyTag},
        schedule::clock,
    },
    random::Seed,
};
use dope_net::{
    link::{egress::data, pool::recv::input, slot::types::CloseGate},
    wire,
    wire::receive::Chunk,
};
use o3::{base::time::Positive, buffer::bytes};
use wire::receive::scoped;

use crate::{
    app::receive::{self, limits},
    network::listener::conn,
};

pub mod failures;
pub(in crate::network::listener) mod policy;
pub(in crate::network::listener) mod raw;
pub mod retire;
mod runtime;
pub mod send;

pub use runtime::Runtime;

pub trait Borrowed<'d, ID: Identity>: Application<'d, ID> {
    fn chunk<'e, R: bytes::TryRetain>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome;
}

/// Listener work requested after consuming a non-escalating egress-drain proof.
/// `Activate` hands scheduling back to the exact generation-branded listener.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub enum PostDrain {
    #[default]
    Idle,
    Activate,
}

/// Listener-owned bounds for one admitted connection generation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lifetime {
    termination: Termination,
}

/// Application-owned timing for one independent listener deadline root.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Timing(Root);

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
enum Root {
    #[default]
    Disabled,
    Timeout(time::Duration),
    Managed,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Termination {
    Persistent,
    Graceful {
        absolute_age: Positive,
        drain_timeout: Positive,
    },
}

impl Lifetime {
    /// Keeps an active connection until an explicit close or the independent
    /// transport, idle, send, or application bounds terminate it.
    pub const fn persistent() -> Self {
        Self {
            termination: Termination::Persistent,
        }
    }

    pub const fn graceful(age: Positive, drain: Positive) -> Self {
        Self {
            termination: Termination::Graceful {
                absolute_age: age,
                drain_timeout: drain,
            },
        }
    }

    pub(super) const fn termination(self) -> Termination {
        self.termination
    }
}

impl Timing {
    pub const fn disabled() -> Self {
        Self(Root::Disabled)
    }

    pub const fn timeout(delay: time::Duration) -> Self {
        Self(Root::Timeout(delay))
    }

    /// Installs one exact application-managed root per connection.
    pub const fn managed() -> Self {
        Self(Root::Managed)
    }

    pub(super) fn combine(self, other: Self) -> Self {
        let root = match (self.0, other.0) {
            (Root::Timeout(left), Root::Timeout(right)) => Root::Timeout(left.min(right)),
            (Root::Timeout(delay), _) | (_, Root::Timeout(delay)) => Root::Timeout(delay),
            (Root::Managed, _) | (_, Root::Managed) => Root::Managed,
            (Root::Disabled, Root::Disabled) => Root::Disabled,
        };
        Self(root)
    }

    pub(super) const fn delay(self) -> Option<time::Duration> {
        match self.0 {
            Root::Timeout(delay) => Some(delay),
            Root::Disabled | Root::Managed => None,
        }
    }

    pub(super) const fn is_managed(self) -> bool {
        !matches!(self.0, Root::Disabled)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Timing>() == std::mem::size_of::<time::Duration>());
    assert!(std::mem::align_of::<Timing>() == std::mem::align_of::<time::Duration>());
};

/// Exact update required to continue after an application deadline expires.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Deadline<'d> {
    Ensure(clock::Moment<'d>),
    Arm(clock::Deadline<'d>),
    Clear,
}

/// Application disposition after consuming its exact expired deadline.
/// Continuing without an explicit next deadline state is not representable:
///
/// ```compile_fail
/// use dope_manifold::network::listener::handler::Timeout;
///
/// fn forget_update<'d>() -> Timeout<'d> {
///     Timeout::Continue
/// }
/// ```
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Timeout<'d> {
    Discard,
    CloseAfter,
    Continue(Deadline<'d>),
}

/// Protocol state after a graceful connection-lifetime drain begins.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Drain {
    Discard,
    Complete,
    Pending,
}

/// Application barrier available only to causal gated receive modes.
/// The prepared deferred lane preserves completions racing the transition;
/// live egress remains an independent built-in receive fence.
pub trait Gated<'d, ID: Identity>: Application<'d, ID> {
    /// Runs after every send owner for this generation retires.
    /// The capability cannot submit another write.
    fn send_drained<'e>(
        self: pin::Pin<&mut Self>,
        conn: conn::Drained<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> PostDrain;
}

/// Application receiving one wire cursor under a callback-scoped mutable
/// borrow. The cursor cannot be moved into connection state; exact retained
/// ranges are authorized by the supplied driver-domain budget.
pub trait Scoped<'w, 'd: 'w, ID: Identity>: Application<'d, ID> {
    /// Returns the receive authority required by this concrete application
    /// instance. Keeping this dynamic lets protocol limits determine the
    /// exact listener-wide budget without a parallel hard-coded constant.
    fn retention(&self) -> limits::Budget;

    fn scoped_chunk<'e>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        chunk: scoped::Input<'_, 'w, 'd, Self::Wire>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome;
}

pub trait Owned<'w, 'd: 'w, ID: Identity>: Application<'d, ID, Input = receive::Owned> {
    const RETENTION: limits::Retention;

    /// Supplies this instance's receive-credit bound once during installation.
    fn retention(&self) -> limits::Retention {
        Self::RETENTION
    }

    fn chunk<'e>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        chunk: Chunk<'w, 'd, Self::Wire>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome;
}

pub trait Application<'d, ID: Identity>: Sized {
    type Conn;
    type Build;
    type Wire: input::Execution + 'd;
    /// Failure consumer bound to the wire owner by `Runtime`.
    type WireFailure;
    /// Compile-time final owner audit after every transport reference retires.
    type Retirement: retire::Policy<'d, Self, ID>;
    /// The single egress representation owned by this application for the
    /// complete generative driver lifetime.
    type Egress: data::raw::Payload<'d> + 'd;
    type Input: receive::Mode<Self::Wire>;
    type Send: send::Policy<'d, ID, Self>;

    /// Closes monotonic application gates synchronously with the listener.
    /// Existing owners remain authoritative while retiring admitted work.
    fn seal(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self> {
        self
    }

    /// Constructs runnable state for one admitted connection.
    /// Capacity-dependent protocols cannot substitute a fake `Default`.
    /// The driver seed supplies connection-local keyed state without new entropy I/O.
    fn connection(self: pin::Pin<&Self>, seed: &Seed) -> Result<Self::Conn, Self::Build>;

    /// Consumes the exact construction failure after admission is rolled back.
    fn build_failed(self: pin::Pin<&mut Self>, cause: Self::Build);

    fn timing(&self) -> Timing {
        Timing::default()
    }

    fn deadline(self: pin::Pin<&Self>, clock: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>>;

    fn timeout<'e>(
        self: pin::Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Timeout<'d> {
        Timeout::Discard
    }

    fn drain<'e>(
        self: pin::Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Drain {
        Drain::Discard
    }

    /// Returns whether the application gate for a gated input mode is closed.
    /// Non-gated input modes never consult this hook.
    fn is_blocked(
        self: pin::Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> bool {
        false
    }

    /// Allows input progress while output is pending when the application can
    /// consume it without retaining input or producing further output.
    fn can_receive(
        self: pin::Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> bool {
        false
    }

    /// Activates application work that became ready without socket progress.
    fn activate<'e, 'a, 'p, 's, 'w>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        conn
    }

    /// Observes exact raw-prefix discard completion with no following input.
    fn discarded<'e>(
        self: pin::Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
    }

    /// Observes orderly input end before teardown.
    /// `Hold` defers application retirement behind `close_gate`. Input end stops
    /// receive admission while previously admitted output drains.
    fn eof<'e>(
        self: pin::Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
        _: receive::End,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Eof {
        Eof::Close
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: pin::Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Self::Wire, Self::Conn, Self::Egress>
    where
        'd: 'w,
    {
        conn
    }

    fn close_gate(
        self: pin::Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> CloseGate {
        CloseGate::Immediate
    }

    fn accept<'e>(
        self: pin::Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Self::Wire, Self::Conn, Self::Egress>,
    ) -> receive::Outcome {
        receive::Outcome::Continue
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Eof {
    #[default]
    Close,
    Hold,
}

/// Why an application egress owner left the listener transport.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Disposition {
    Sent,
    Closed,
}

/// Exact connection, owner, and disposition released by the listener.
pub struct Release<'d, ID: Identity, B> {
    connection: conn::Id<'d, ID>,
    body: B,
    disposition: Disposition,
}

impl<'d, ID: Identity, B> Release<'d, ID, B> {
    pub(in crate::network::listener) const fn new(
        conn: conn::Id<'d, ID>,
        body: B,
        cause: Disposition,
    ) -> Self {
        Self {
            connection: conn,
            body,
            disposition: cause,
        }
    }

    pub fn into_parts(self) -> (conn::Id<'d, ID>, B, Disposition) {
        (self.connection, self.body, self.disposition)
    }
}

const _: () = assert!(
    std::mem::size_of::<Release<'static, KeyTag<0>, usize>>()
        == std::mem::size_of::<(conn::Id<'static, KeyTag<0>>, usize, Disposition)>()
);
