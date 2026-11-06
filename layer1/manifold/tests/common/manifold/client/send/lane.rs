use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
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
    link::{egress, pool::change::opening},
    wire,
};
use dope_test::harness::signals::{Gate, Progress as _};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        error::{self, Error::Send},
        send::{Error, load, wire::Wire},
    },
};

#[derive(Clone, Default)]
pub struct Signals {
    pub(crate) conn: Gate,
    pub(crate) moved: Gate,
    pub(crate) sent: Gate,
    pub(crate) closed: Gate,
}

pub struct Lane {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) pending: [Cell<Option<egress::NonEmpty<Shared>>>; 3],
    pub(crate) signals: Signals,
    pub(crate) abort: Cell<bool>,
}

impl<'d> Session<'d> for Lane {
    type Codec = codec::Pending<5>;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Requests = request::Enabled;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }
}

impl<'d> policy::Compatible<'d, 'd, Wire, KeyTag<0>> for Lane {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Lane {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Lane {
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

impl<'d> Output<'d> for Lane {
    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible, Infallible, Infallible, _>::Queue(
            |(owner, _, pending): (&mut Self, conn::Id<'d, KeyTag<0>>, egress::Pending)| {
                match pending {
                    egress::Pending::Absent => owner.signals.sent.hit(),
                    egress::Pending::Present => {}
                }
            },
        )
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<Infallible, _>::Owner(|(owner, payload): (&mut Self, Self::Send)| {
            if payload.as_ref() != b"x" {
                owner
                    .audit
                    .faults
                    .record(Send(Error::Bytes(payload.as_ref().to_vec())));
            }
        })
    }
}

impl<'d> Source<'d> for Lane {
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
        let (_, mut requests) = turn.parts();
        let index = requests.connection().index().into_usize();
        let slot = &self.pending[index];
        let Some(value) = slot.take() else {
            return;
        };
        let close = match requests.admit(value) {
            Ok(request::Admission::Ready(ready)) => {
                ready.push();
                self.signals.moved.hit();
                false
            }
            Err(value) => {
                slot.set(Some(value));
                false
            }
            Ok(request::Admission::Saturated(load)) => {
                slot.set(Some(load));
                self.abort.replace(false)
            }
            Ok(request::Admission::Rejected(rejected)) => {
                self.audit
                    .faults
                    .record(error::Error::Admit(rejected.cause()));
                return;
            }
        };
        if close {
            requests.close(Directive::cut_final(Reason::Local(
                LocalOrigin::ResourceFailure,
            )));
        }
    }
}

impl<'d> session::Connected<'d> for Lane {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let index = context.connection().index().into_usize();
        let slot = &self.pending[index];
        slot.set(Some(load(b"x")));
        self.signals.conn.hit();
    }
}

impl<'d> session::Failed<'d> for Lane {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Lane {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Lane {
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
        _: &mut (),
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.pending[context.connection().index().into_usize()].take();
        self.signals.closed.hit();
        let actual = self.signals.closed.hits() as usize;
        let maximum = self.signals.conn.hits() as usize;
        if actual > maximum {
            self.audit
                .faults
                .record(Send(Error::Closed { actual, maximum }));
        }
    }
}

impl<'d> Scheduling<'d> for Lane {}

impl<'d> Target<'d, KeyTag<0>, 2> for Lane {}
impl<'d> Target<'d, KeyTag<0>, 3> for Lane {}
