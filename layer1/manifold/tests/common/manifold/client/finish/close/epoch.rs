use std::{convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt,
    close::{LocalOrigin, Reason},
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
    link::{egress, pool::change::opening},
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
        finish::{Error, shutdown::CONNECTIONS},
        flush,
    },
};

pub struct Epoch<const SHUTDOWN: bool = false> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) conn: Gate,
    pub(crate) closed: Gate,
}

impl<'d, const SHUTDOWN: bool> Session<'d> for Epoch<SHUTDOWN> {
    type Codec = codec::Pending;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Receipt = ();
    type Requests = request::Disabled;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }
}

impl<'d, const SHUTDOWN: bool> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Epoch<SHUTDOWN> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d, const SHUTDOWN: bool> Parser<'d> for Epoch<SHUTDOWN> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, const SHUTDOWN: bool> Plain<'d> for Epoch<SHUTDOWN> {
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

impl<'d, const SHUTDOWN: bool> Output<'d> for Epoch<SHUTDOWN> {
    fn flush() -> Option<
        impl for<'a, 'c, 't, 'e> Call<(
            &'a mut Self,
            &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
            dope_manifold::client::session::Input,
        )>,
    > {
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

impl<'d, const SHUTDOWN: bool> session::Connected<'d> for Epoch<SHUTDOWN> {
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

impl<'d, const SHUTDOWN: bool> session::Failed<'d> for Epoch<SHUTDOWN> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, const SHUTDOWN: bool> session::Exhausted<'d> for Epoch<SHUTDOWN> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d, const SHUTDOWN: bool> Retirement<'d> for Epoch<SHUTDOWN> {
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
        reason: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let conn = self.conn.hits();
        if SHUTDOWN {
            if reason != Reason::Local(LocalOrigin::RuntimeShutdown) || conn != CONNECTIONS as u32 {
                self.audit.faults.record(Finish(Error::ShutdownRetired {
                    reason,
                    connected: conn,
                }));
            }
            return;
        }
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
impl<'d> Scheduling<'d> for Epoch<true> {
    fn shutdown() -> Option<impl for<'v> Call<&'v mut Self>> {
        Some(|session: &mut Self| {
            let conn = session.conn.hits();
            let closed = session.closed.hits();
            if conn != CONNECTIONS as u32 || closed != 0 {
                session.audit.faults.record(Finish(Error::Shutdown {
                    connected: conn,
                    closed,
                }));
            }
        })
    }
}

impl<'d> Target<'d, KeyTag<0>, 2> for Epoch {}
impl<'d> Target<'d, KeyTag<0>, CONNECTIONS> for Epoch<true> {}
