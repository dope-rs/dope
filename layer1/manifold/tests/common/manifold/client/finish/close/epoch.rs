use std::{convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt,
    close::Reason,
    conn,
    session::{
        self, Output, Parser, Plain, Retirement, Scheduling, Session, Target,
        app::drain::Ready,
        codec::mode,
        policy::{self, output},
        response, turns,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::Identity,
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
        error::{self, Error::Finish},
        finish::Error,
        flush,
    },
};

pub struct Epoch {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) conn: Gate,
    pub(crate) closed: Gate,
}

impl<'d> Session<'d> for Epoch {
    type Codec = codec::Pending;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Requests = request::Disabled;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }
}

impl<'d> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Epoch {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Epoch {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Epoch {
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
        _: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        match head {}
    }
}

impl<'d> Output<'d> for Epoch {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            if !context.sink.is_empty() {
                owner.audit.faults.record(Finish(Error::Flush {
                    connected: owner.conn.hits(),
                    closed: owner.closed.hits(),
                    empty: false,
                }));
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

impl<'d> session::Connected<'d> for Epoch {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, (), Shared>,
    ) {
        self.conn.hit();
    }
}

impl<'d> session::Failed<'d> for Epoch {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Epoch {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Epoch {
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
        let conn = self.conn.hits();
        let closed = self.closed.hits();
        if conn <= closed {
            self.audit.faults.record(Finish(Error::Epoch {
                connected: conn,
                closed,
            }));
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, (), KeyTag<0>, Self>) {
        self.closed.hit();
        let conn = self.conn.hits();
        let closed = self.closed.hits();
        if closed > conn {
            self.audit.faults.record(Finish(Error::Epoch {
                connected: conn,
                closed,
            }));
        }
    }
}

impl<'d> Scheduling<'d> for Epoch {}
impl<'d> Target<'d, KeyTag<0>, 2> for Epoch {}
