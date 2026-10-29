use std::{cell::Cell, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Controlled, request},
    attempt,
    close::Reason,
    conn,
    session::{
        self, Output, Parser, Plain, Retirement, Scheduling, Target,
        app::drain::Ready,
        codec::{Codec, Stateless, eof, mode},
        policy::{self, output},
        response, turns,
    },
};
use dope_net::{
    link::{egress, pool::change::opening},
    wire,
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        error::{self, Error::Finish},
        finish::Error,
        flush,
    },
};

#[derive(Clone)]
pub struct Events<'d> {
    pub(crate) conn: Vec<conn::Id<'d, KeyTag<0>>>,
    pub(crate) peers: Vec<socket::Addr>,
    pub(crate) closed: Vec<(conn::Id<'d, KeyTag<0>>, Reason)>,
    pub(crate) quarantined: Vec<(conn::Id<'d, KeyTag<0>>, usize, usize)>,
    pub(crate) replies: Option<usize>,
}

impl Default for Events<'_> {
    fn default() -> Self {
        Self {
            conn: Vec::new(),
            peers: Vec::new(),
            closed: Vec::new(),
            quarantined: Vec::new(),
            replies: Some(0),
        }
    }
}

impl Events<'_> {
    /// Restores the live history before returning its independent observation.
    pub fn snapshot(events: &Cell<Self>) -> Self {
        let recorded = events.take();
        let observed = recorded.clone();
        events.set(recorded);
        observed
    }

    pub fn is_ordered(&self) -> bool {
        self.quarantined.iter().all(|(conn, count, closed)| {
            self.conn
                .get(..*count)
                .is_some_and(|prior| prior.contains(conn))
                && self
                    .closed
                    .get(..*closed)
                    .is_some_and(|prior| prior.iter().all(|(id, _)| id != conn))
        })
    }
}

pub struct Session<'d, C: Codec> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) codec: C,
    pub(crate) errors: Rc<Trace<C::Error, 16>>,
    pub(crate) events: Rc<Cell<Events<'d>>>,
    pub(crate) gate: Gate,
}

impl<'d, C: Codec> session::Session<'d> for Session<'d, C> {
    type Codec = C;
    type Connect = Full;
    type ConnState = ();
    type Failure = Controlled;
    type Requests = request::Disabled;
    type Send = Shared;

    fn codec(&self) -> &C {
        &self.codec
    }
}

impl<'d, C: Codec> policy::Compatible<'d, 'd, wire::Identity, KeyTag<0>> for Session<'d, C> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d, C> Parser<'d> for Session<'d, C>
where
    C: Codec + Stateless + eof::Finish,
{
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, C> Plain<'d> for Session<'d, C>
where
    C: Codec + Stateless + eof::Finish,
{
    type Delivery = response::Direct;

    fn reject(
        &mut self,
        cause: C::Error,
        ctx: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> response::Direct {
        use dope_manifold::client::close;

        self.errors.record(cause);
        ctx.request(close::Directive::reconnect(Reason::Protocol));
        response::Direct::Committed
    }

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        _: C::Head<'input, 'd, R>,
        _: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        let mut events = self.events.take();
        events.replies = events.replies.and_then(|count| count.checked_add(1));
        self.events.set(events);
        response::Direct::Committed
    }
}

impl<'d, C: Codec> Output<'d> for Session<'d, C> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            if !context.sink.is_empty() {
                owner.audit.faults.record(Finish(Error::NonemptyOutput));
            }
        })
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<_>::Ready(|owner: &mut Self| {
            owner.audit.record_sent();
        })
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        use std::convert::Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d, C: Codec> session::Connected<'d> for Session<'d, C> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        peer: socket::Addr,
        context: turns::Startup<'_, '_, 'd, (), Shared>,
    ) {
        let conn = context.connection();
        let mut events = self.events.take();
        events.conn.push(conn);
        events.peers.push(peer);
        self.events.set(events);
        self.gate.hit();
    }
}

impl<'d, C: Codec> session::Exhausted<'d> for Session<'d, C> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d, C: Codec> Retirement<'d> for Session<'d, C> {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        error: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn quarantine(
        &self,
        conn: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let mut events = self.events.take();
        let count = events.conn.len();
        let closed = events.closed.len();
        events.quarantined.push((conn, count, closed));
        self.events.set(events);
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, (), KeyTag<0>, Self>) {
        let retired = (context.connection(), context.reason());
        let mut events = self.events.take();
        events.closed.push(retired);
        let conn = events.conn.len();
        let closed = events.closed.len();
        self.events.set(events);
        if closed > conn {
            self.audit
                .faults
                .record(Finish(Error::Closed { conn, closed }));
        }
    }
}

impl<'d, C: Codec> Scheduling<'d> for Session<'d, C> {}

impl<'d, C, const MAX: usize> Target<'d, KeyTag<0>, MAX> for Session<'d, C> where
    C: Codec + Stateless + eof::Finish
{
}
