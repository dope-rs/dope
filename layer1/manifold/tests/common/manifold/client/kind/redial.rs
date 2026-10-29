use std::{cell::Cell, convert::Infallible, rc::Rc, time::Duration};

use dope_core::{
    driver,
    driver::{result::SubmitError, route::KeyTag},
    io::socket,
};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt,
    close::{Directive, LocalOrigin, Reason},
    conn,
    session::{
        self, Output, Parser, Plain, Retirement, Scheduling, Session, Target,
        app::{drain::Ready, request::Source},
        codec::mode,
        policy::{self, output},
        response, turns,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{self, Identity},
};
use dope_test::harness::{
    observe::Trace,
    signals::{LocalGateRef, Progress as _},
};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        error::{self, Error::Kind},
        flush,
        kind::Error,
    },
};

pub type Exhaustion = (opening::Exhaustion, u32, u32);

pub const MAX: usize = 1;
pub const BACKOFF: Duration = Duration::from_millis(20);

#[derive(Clone, Copy)]
pub enum Mode {
    Reconnect,
    Permanent,
    Exhausted(SubmitError),
}

pub struct Redial<'test> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) kind: Mode,
    pub(crate) conn: LocalGateRef<'test>,
    pub(crate) begun: LocalGateRef<'test>,
    pub(crate) steps: LocalGateRef<'test>,
    pub(crate) closed: LocalGateRef<'test>,
    pub(crate) exhausted: Option<(LocalGateRef<'test>, &'test Trace<Exhaustion, 2>)>,
    pub(crate) pending: Cell<bool>,
}

impl<'d, 'test> Session<'d> for Redial<'test> {
    type Codec = codec::Pending;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Requests = request::Enabled;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }
}

impl<'d, 'test> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Redial<'test> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d, 'test> Parser<'d> for Redial<'test> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, 'test> Plain<'d> for Redial<'test> {
    type Delivery = response::Direct;

    fn reject(
        &mut self,
        _: errors::Truncated,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct {
        use dope_manifold::client::close;

        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
        response::Direct::Committed
    }

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        head: Infallible,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        match head {}
    }
}

impl<'d, 'test> Output<'d> for Redial<'test> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let pending = owner.pending.get();
            let empty = context.sink.is_empty();
            if pending || !empty {
                owner
                    .audit
                    .faults
                    .record(Kind(Error::Flush { pending, empty }));
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
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d, 'test> Source<'d> for Redial<'test> {
    fn drain<'w, 'e, 'turn, W: wire::Spec + 'd>(
        &self,
        turn: &mut request::Turn<
            'turn,
            'e,
            'd,
            session::Connection<'w, 'd, KeyTag<0>, Self, W>,
            Self::Send,
        >,
    ) where
        'd: 'w,
    {
        if self.pending.replace(false) {
            turn.close(match self.kind {
                Mode::Reconnect => Directive::reconnect(Reason::Local(LocalOrigin::ApiRequest)),
                Mode::Permanent => Directive::permanent(Reason::Local(LocalOrigin::ApiRequest)),
                Mode::Exhausted(error) => {
                    Directive::cut_final(Reason::Receive(event::ParkError::Submit(error)))
                }
            });
        }
    }
}

impl<'d, 'test> session::Connected<'d> for Redial<'test> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.conn.hit();
        if self.conn.hits() == 1 {
            self.pending.set(true);
        }
    }
}

impl<'d, 'test> session::Failed<'d> for Redial<'test> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, 'test> session::Exhausted<'d> for Redial<'test> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        if let Some((notice, events)) = self.exhausted {
            events.record((error, self.conn.hits(), self.closed.hits()));
            notice.hit();
        } else {
            self.audit.faults.record(error::Error::Exhaust(error));
        }
    }
}

impl<'d, 'test> Retirement<'d> for Redial<'test> {
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
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut Self::ConnState,
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        self.begun.hit();
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn clear_recv<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: Reason,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        self.steps.hit();
        if self.steps.hits() == 1 {
            egress::Cleared::Retry
        } else {
            egress::Cleared::Done
        }
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.closed.hit();
        let closed = self.closed.hits();
        let conns = self.conn.hits();
        if closed > conns {
            self.audit
                .faults
                .record(Kind(Error::Closed { conns, closed }));
        }
    }
}

impl<'d, 'test> Scheduling<'d> for Redial<'test> {}
impl<'d, 'test, const N: usize> Target<'d, KeyTag<0>, N> for Redial<'test> {}

pub struct Run {
    pub conns: u32,
    pub begun: u32,
    pub steps: u32,
    pub closed: u32,
    pub accepts: usize,
}
