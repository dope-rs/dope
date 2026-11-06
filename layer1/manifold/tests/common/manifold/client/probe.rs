use std::{
    cell::Cell,
    convert::Infallible,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::{
    driver,
    driver::{route::KeyTag, schedule::ready},
    io::socket,
};
use dope_manifold::client::{
    app,
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
    link::{egress, pool::change::opening},
    wire::{Identity, failure, open},
};
use dope_test::harness::signals::{Gate, LocalGateRef, Progress as _};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{connect::open::First, error, flush},
};

pub const MAX: usize = 1;
pub const IDLE_TIMEOUT: Positive = Positive::from_millis::<150>();
pub const REDIAL_BACKOFF: Duration = Duration::from_millis(20);

#[derive(Debug)]
pub enum Error {
    Flush { connected: u32, empty: bool },
    Quarantine(u32),
    Disconnected(u32),
}

pub struct Probe<'test, F> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) timeout: Option<Positive>,
    pub(crate) peer: F,
    pub(crate) gate: LocalGateRef<'test>,
    pub(crate) start: Instant,
    pub(crate) redial: &'test Cell<Option<Duration>>,
    pub(crate) rejected: Option<&'test dyn Fn()>,
    pub(crate) expired: Option<&'test Gate>,
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Session<'d> for Probe<'_, F> {
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

impl<'test, 'd, F: Fn(socket::Addr, ready::Target<'d>)>
    policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Probe<'test, F>
{
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> policy::Compatible<'d, 'd, First, KeyTag<0>>
    for Probe<'_, F>
{
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)>
    policy::start::Sink<'d, 'd, First, KeyTag<0>, failure::Possible> for Probe<'_, F>
{
    fn fail(
        &self,
        _: attempt::Completion<'_, 'd>,
        _: open::Error<'d, First>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.rejected.expect("opening rejection observer")();
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Parser<'d> for Probe<'_, F> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Plain<'d> for Probe<'_, F> {
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

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Output<'d> for Probe<'_, F> {
    fn flush() -> Option<
        impl for<'a, 'c, 't, 'e> Call<(
            &'a mut Self,
            &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
            dope_manifold::client::session::Input,
        )>,
    > {
        flush(|owner: &mut Self, context| {
            let conn = owner.gate.hits();
            let empty = context.sink.is_empty();
            if conn == 0 || !empty {
                owner.audit.faults.record(error::Error::Probe(Error::Flush {
                    connected: conn,
                    empty,
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
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> session::Connected<'d> for Probe<'_, F> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        peer: socket::Addr,
        context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        (self.peer)(peer, context.ready);
        self.gate.hit();
        if self.gate.hits() == 2 {
            self.redial.set(Some(self.start.elapsed()));
        }
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> session::Failed<'d> for Probe<'_, F> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> session::Exhausted<'d> for Probe<'_, F> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Retirement<'d> for Probe<'_, F> {
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
        if let Some(expired) = self.expired
            && matches!(
                reason,
                Reason::Timeout(dope_manifold::client::deadline::Kind::Inbound)
            )
        {
            expired.hit();
        }
        if self.gate.hits() == 0 {
            self.audit
                .faults
                .record(error::Error::Probe(Error::Quarantine(0)));
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        if self.gate.hits() == 0 {
            self.audit
                .faults
                .record(error::Error::Probe(Error::Disconnected(0)));
        }
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Scheduling<'d> for Probe<'_, F> {
    fn inbound(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
        _: Positive,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        self.timeout
            .map_or(app::Inbound::Quiescent, app::Inbound::Awaiting)
    }
}

impl<'d, F: Fn(socket::Addr, ready::Target<'d>)> Target<'d, KeyTag<0>, MAX> for Probe<'_, F> {}
