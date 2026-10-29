use std::{convert::Infallible, rc::Rc, time::Duration};

use dope_core::{
    driver::{self, route::KeyTag, settings},
    io::socket,
};
use dope_manifold::{
    client::{
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
    },
    timing::Policy,
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::Identity,
};
use dope_test::harness::{
    observe::Trace,
    signals::{LocalGateRef, Progress as _},
};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{error, flush},
};

pub const MAX: usize = 1;
pub const LIMIT: Positive = Positive::from_millis::<120>();

pub struct Profile;

impl settings::Profile for Profile {
    const QUEUES: settings::QueueLayout = settings::QueueLayout::fixed::<64, 65_536>();
}

impl Policy for Profile {
    const CONNECT_DEADLINE: Positive = Positive::from_secs::<2>();
    const IDLE_WINDOW: Positive = Positive::from_secs::<2>();
    const SEND_DEADLINE: Positive = Positive::from_secs::<2>();
}

pub struct Lifetime<'test> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) ready: LocalGateRef<'test>,
    pub(crate) events: &'test Trace<Event, 16>,
}

#[derive(Debug)]
pub enum Event {
    Connected(Option<Option<Duration>>),
    Quarantined(u32),
    Closed(Reason),
    Flush { ready: u32, empty: bool },
}

impl<'d> Session<'d> for Lifetime<'_> {
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

impl<'test, 'd> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Lifetime<'test> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Lifetime<'_> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Lifetime<'_> {
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

impl<'d> Output<'d> for Lifetime<'_> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let ready = owner.ready.hits();
            let empty = context.sink.is_empty();
            if ready == 0 || !empty {
                owner.events.record(Event::Flush { ready, empty });
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

impl<'d> session::Connected<'d> for Lifetime<'_> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let now = context.driver.driver_ref().clock().now();
        self.events.record(Event::Connected(
            context.lifetime.expires_at().map(|at| at.try_left(now)),
        ));
        self.ready.hit();
    }
}

impl<'d> session::Failed<'d> for Lifetime<'_> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Lifetime<'_> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Lifetime<'_> {
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
        self.events.record(Event::Quarantined(self.ready.hits()));
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.events.record(Event::Closed(context.reason()));
    }
}

impl<'d> Scheduling<'d> for Lifetime<'_> {}

impl<'d> Target<'d, KeyTag<0>, MAX> for Lifetime<'_> {}
