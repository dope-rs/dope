use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt,
    close::Reason,
    conn, deadline,
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
    wire::{self},
};
use dope_test::harness::{observe::Trace, signals::Gate};
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

pub const MAX: usize = 1;
pub const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

pub struct Wait {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) pending: [Cell<Option<egress::NonEmpty<Shared>>>; 2],
    pub(crate) index: Rc<Cell<Option<usize>>>,
    pub(crate) reasons: Rc<Trace<(usize, Reason), 2>>,
    pub(crate) connected: Gate,
    pub(crate) seen: Gate,
    pub(crate) closed: Gate,
}
impl<'d> Session<'d> for Wait {
    type Codec = codec::Pending;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Receipt = ();
    type Requests = request::Enabled;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }
}

impl<'d> policy::Compatible<'d, 'd, Wire, KeyTag<0>> for Wait {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Wait {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}
impl<'d> Plain<'d> for Wait {
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
impl<'d> Output<'d> for Wait {
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
impl<'d> Source<'d> for Wait {
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
        let (mut requests, _) = turn.requests();
        let index = requests.connection().index().into_usize();
        let slot = &self.pending[index];
        let Some(value) = slot.take() else {
            return;
        };
        match requests.admit(value) {
            Ok(request::Admission::Ready(ready)) => ready.push(),
            Ok(request::Admission::Saturated(load)) => {
                slot.set(Some(load));
                if self.index.get().is_none() {
                    self.index.set(Some(index));
                    self.seen.hit();
                }
            }
            Err(value) => {
                slot.set(Some(value));
            }
            Ok(request::Admission::Rejected(rejected)) => {
                self.audit
                    .faults
                    .record(error::Error::Admit(rejected.cause()));
            }
        }
    }
}
impl<'d> session::Connected<'d> for Wait {
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
        self.connected.hit();
    }
}
impl<'d> session::Failed<'d> for Wait {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}
impl<'d> session::Exhausted<'d> for Wait {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}
impl<'d> Retirement<'d> for Wait {
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
        reason: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        if self.index.get() == Some(conn.index().into_usize())
            && reason != Reason::Timeout(deadline::Kind::Send)
        {
            self.audit.faults.record(Send(Error::Quarantine(reason)));
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        let index = context.connection().index().into_usize();
        self.reasons.record((index, context.reason()));
        if self.index.get() == Some(index) {
            self.closed.hit();
        }
    }
}
impl<'d> Scheduling<'d> for Wait {}

impl<'d> Target<'d, KeyTag<0>, 2> for Wait {}
