use std::{convert::Infallible, marker, mem};

use codec::{mode, staging, staging::Seal};
use dope_core::{
    driver::{
        self,
        route::{Identity, KeyTag},
        schedule,
        schedule::clock,
    },
    io::{socket, transfer},
};
use dope_net::{
    link::{
        egress::{self, data},
        event,
        pool::change::opening,
    },
    wire,
};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer::{self, hold, resident, write::consume::scope},
    mem::rc,
};
use policy::output;
use response::{
    flow::{Begin, Policy as _},
    work,
};

use crate::{
    app::Park,
    client::{
        app::{Eof, Fence, Inbound, admit, connect, failure, request::Mode},
        attempt, close,
        conn::Id,
        session::app::{admit::Register, drain},
    },
};

/// Each protocol selects the connection state and payload admitted by its turn.
type Context<'t, 'e, 'life, 'd, P, S, ID, const MAX: usize> = turns::Context<
    't,
    'e,
    'd,
    <S as protocol::Spec<'life, 'd, P, ID, MAX>>::ConnState,
    <S as protocol::Spec<'life, 'd, P, ID, MAX>>::Send,
    ID,
>;

pub mod app;
pub mod codec;
pub mod ingress;
pub mod policy;
pub mod protocol;
pub mod response;
mod setup;
pub mod turns;

pub use setup::Setup;

type Ingress<'w, 'd, ID, N, W> = mode::Ingress<'w, 'd, ID, N, W>;

/// Parser, ingress, and response state owned by one connection.
pub struct Connection<'w, 'd: 'w, ID: Identity, N: Parser<'d, ID>, W: wire::Spec + 'd> {
    pub(super) ingress: Ingress<'w, 'd, ID, N, W>,
    pub(in crate::client) conn_state: N::ConnState,
    pub(super) continuation: <N::Response as response::Plan>::Flow,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'w, 'd: 'w, ID: Identity, N: Parser<'d, ID>, W: wire::Spec + 'd> Connection<'w, 'd, ID, N, W> {
    pub(super) fn try_new(
        session: &N,
        pool: &<<N::Codec as codec::Codec>::Staging as Seal>::Pool<'d>,
        budget: resident::Budget<'d>,
    ) -> Result<Self, rc::Error> {
        let stage = <<N::Codec as codec::Codec>::Staging as Seal>::Buf::try_from(pool)?;
        Ok(Self {
            ingress: mode::Ingress::new(stage, session.codec(), budget),
            conn_state: N::ConnState::default(),
            continuation: Default::default(),
            driver: marker::PhantomData,
        })
    }

    pub(crate) fn state_mut(&mut self) -> &mut N::ConnState {
        &mut self.conn_state
    }

    pub(super) fn activate(&mut self, codec: &N::Codec) -> &mut N::ConnState {
        self.ingress.discard(codec);
        self.continuation.reset();
        &mut self.conn_state
    }
}

impl<'w, 'd: 'w, ID: Identity, N, W> Connection<'w, 'd, ID, N, W>
where
    N: Parser<'d, ID, Mode = mode::Stored>
        + Stored<'d, ID, Delivery = <N::Response as response::Plan>::Delivery>,
    N::Codec: codec::Stateful,
    W: wire::Spec + 'd,
{
    pub fn parser(&mut self) -> &mut <N::Codec as codec::Stateful>::State<'d> {
        self.ingress.state_mut()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Settlement {
    Ready,
    Yield,
    Blocked,
}

/// Selects framed, paused, or opaque receive ownership for one generation.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub enum ReceiveMode {
    #[default]
    Framed,
    Paused,
    Duplex,
}

/// Settlement for one opaque receive callback.
#[must_use]
pub enum Duplex<'id> {
    Consumed(scope::End<'id, { transfer::MAX_BYTES as u32 }>),
    Failed(scope::Error),
    Blocked,
    Close,
}

pub trait Session<'d, ID: Identity = KeyTag<0>>: Sized {
    type Codec: codec::Codec;
    type Connect: connect::Mode;
    type ConnState: Default;
    type Failure: failure::Mode;
    type Requests: Mode;
    type Send: data::raw::Payload<'d>;

    fn codec(&self) -> &Self::Codec;

    /// Selects framed, transition-barrier, or opaque receive semantics for
    /// one exact connection generation. The default adds no connection state.
    fn mode(&self, _: Id<'d, ID>, _: &Self::ConnState) -> ReceiveMode {
        ReceiveMode::Framed
    }

    /// Receives bytes after this generation has committed to duplex mode.
    /// `Blocked` leaves the cursor and its retained owner untouched so an
    /// activation can retry the same bytes after downstream capacity returns.
    fn duplex<'id, 'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        _: codec::Input<'id, 'input, 'd, R>,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> Duplex<'id>
    where
        'd: 'input,
    {
        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
        Duplex::Close
    }

    /// Observes transport EOF after duplex mode has consumed every retained
    /// byte. Framed codecs keep their independent `Codec::finish` contract.
    fn finish<'e>(
        &mut self,
        _: close::Reason,
        _: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> Settlement {
        Settlement::Ready
    }

    /// Observes duplex EOF and selects the write-half disposition.
    fn eof<'e>(
        &mut self,
        _: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> Eof {
        Eof::Close
    }
}

/// Owns one response action that runs only after input commitment.
pub trait Commit<'d, P, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    fn commit<'e>(
        &mut self,
        item: response::Committed<P>,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    );
}

/// Owns response work that can yield or block after input commitment.
pub trait Resume<'d, P, Q, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    fn commit<'e, A: work::Admission>(
        &mut self,
        item: response::Committed<P>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> response::Settlement<Q>;

    fn settle<'e, A: work::Admission>(
        &mut self,
        item: response::Item<Q>,
        work: &mut A,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> response::Settlement<Q>;
}

/// Owns callbacks for a codec with no connection-local parser state.
pub trait Plain<'d, ID: Identity = KeyTag<0>>: Session<'d, ID>
where
    Self::Codec: codec::Stateless,
{
    type Delivery: response::Outcome;

    fn deliver<'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        head: <Self::Codec as codec::Codec>::Head<'input, 'd, R>,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> Self::Delivery
    where
        'd: 'input;

    fn reject<'e>(
        &mut self,
        error: <Self::Codec as codec::Codec>::Error,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    ) -> Self::Delivery;
}

/// Owns callbacks for a codec with connection-local parser state.
pub trait Stored<'d, ID: Identity = KeyTag<0>>: Session<'d, ID>
where
    Self::Codec: codec::Stateful,
{
    type Delivery: response::Outcome;

    fn blocked(
        &self,
        _: Id<'d, ID>,
        _: &mut <Self::Codec as codec::Stateful>::State<'d>,
    ) -> Blocking;

    fn decode<S>(
        &mut self,
        conn: Id<'d, ID>,
        storage: &mut <Self::Codec as codec::Stateful>::Storage<S>,
        state: &mut <Self::Codec as codec::Stateful>::State<'d>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) -> codec::Step<Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume;

    fn finish<'slot, 'id, S>(
        &mut self,
        input: codec::Owner<'slot, 'id, 'd, Self::Codec, S>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) -> codec::Finished<codec::Owner<'slot, 'id, 'd, Self::Codec, S>, Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume;
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Blocking {
    Registered,
    Unsupported,
}

/// Binds one session to its statically selected parser storage contract.
pub trait Parser<'d, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    type Mode: mode::Policy<'d, ID, Self, Self::Response>;
    type Response: response::Mode<'d, ID, Self, Flow: Begin<'d, ID, Self, Self::Response>>;
}

/// Owns connection-scoped egress callbacks.
pub trait Output<'d, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send, ID>,
    )>;

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Id<'d, ID>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible>::None
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<_>::Release(mem::drop::<Self::Send>)
    }
}

/// Owns the ordered retirement of one exact connection generation.
///
/// This is a static capability: it adds no runtime object or session state.
pub trait Retirement<'d, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    type Drain: drain::Mode<'d, ID, Self>;

    /// Disposes the exact transport receive failure before transport teardown.
    fn io_fail<'e>(
        &mut self,
        error: dope_core::Errno,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    );

    /// Quarantines the exact connection generation before any multi-turn
    /// request or response retirement begins.
    fn quarantine(
        &self,
        conn: Id<'d, ID>,
        state: &mut Self::ConnState,
        reason: close::Reason,
        region: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    );

    fn clear_send<'turn>(
        &self,
        conn: Id<'d, ID>,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared;

    /// Retry retains the exact connection generation in close phase without releasing its slot.
    fn clear_recv<'turn>(
        &self,
        _: Id<'d, ID>,
        _: close::Reason,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn fence(
        &self,
        _: Id<'d, ID>,
        _: &Self::ConnState,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Fence {
        Fence::Ready
    }

    /// Classifies a fully retired exact transport generation before redial.
    fn policy(
        &self,
        _: Id<'d, ID>,
        _: &Self::ConnState,
        _: close::Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> close::Policy {
        close::Policy::Reconnect
    }

    fn disconnect(&mut self, ctx: turns::Retired<'_, 'd, Self::ConnState, ID, Self>);
}

pub trait Connected<'d, ID: Identity = KeyTag<0>>:
    Session<'d, ID, Connect = connect::Full> + Register<'d, ID, Self::Activation>
{
    type Activation: admit::Mode;

    fn connect<'e>(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        peer: socket::Addr,
        ctx: turns::Startup<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    );
}

pub trait Peer<'d, ID: Identity = KeyTag<0>>: Session<'d, ID, Connect = connect::Peer> {
    type Addr;

    fn connect<'e>(
        &mut self,
        peer: Self::Addr,
        ctx: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send, ID>,
    );
}

pub trait Exhausted<'d, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    fn exhaust(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        driver: &mut driver::Context<'_, 'd>,
    );
}

pub trait Failed<'d, ID: Identity = KeyTag<0>>:
    Session<'d, ID, Failure = failure::Observed>
{
    fn failed(
        &mut self,
        key: attempt::Completion<'_, 'd, ID>,
        cause: event::ConnectFault,
        driver: &mut driver::Context<'_, 'd>,
    );
}

/// Supplies executor-facing scheduling policy for a protocol session.
///
/// This is a static capability: it adds no runtime object or session state.
pub trait Scheduling<'d, ID: Identity = KeyTag<0>>: Session<'d, ID> {
    fn park() -> Option<impl for<'v, 'w, 'x, 'y> Call<(&'v mut Self, &'w mut Park<'x, 'y, 'd>)>> {
        None::<Infallible>
    }

    fn shutdown() -> Option<impl for<'v> Call<&'v mut Self>> {
        None::<Infallible>
    }

    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }

    fn inbound(
        &self,
        _: Id<'d, ID>,
        _: &Self::ConnState,
        _: Positive,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Inbound {
        Inbound::Quiescent
    }
}

/// Proves that a session supports one exact service route and connection bound.
/// The four independent capabilities grant this composite proof automatically.
/// Route identity and capacity are independent compile-time dimensions.
pub trait Target<'d, ID: Identity, const MAX_CONNECTIONS: usize>:
    Parser<'d, ID> + Output<'d, ID> + Retirement<'d, ID> + Scheduling<'d, ID>
{
}

const _: () = assert!(
    mem::size_of::<Setup<'static, (), ingress::None>>()
        == mem::size_of::<ingress::Plan<ingress::None>>()
);
