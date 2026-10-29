use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt,
    close::{self, Reason},
    conn, deadline,
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
    wire::{self, failure},
};
use dope_test::harness::signals::{LocalGateRef, Progress as _};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        error::{self, Error::Send},
        send::Error,
    },
};

pub struct Flight<'a> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) payload: Option<Shared>,
    pub(crate) canceled: LocalGateRef<'a>,
    pub(crate) closed: LocalGateRef<'a>,
    pub(crate) reason: &'a Cell<Option<Reason>>,
    closing: Option<(LocalGateRef<'a>, bool)>,
}

impl<'a> Flight<'a> {
    pub fn new(
        payload: Shared,
        canceled: LocalGateRef<'a>,
        closed: LocalGateRef<'a>,
        reason: &'a Cell<Option<Reason>>,
        audit: Rc<Report<error::Error, 16>>,
    ) -> Self {
        Self {
            audit,
            payload: Some(payload),
            canceled,
            closed,
            reason,
            closing: None,
        }
    }

    pub fn closing(mut self, ready: LocalGateRef<'a>, shutdown: bool) -> Self {
        self.closing = Some((ready, shutdown));
        self
    }
}

impl<'d> Session<'d> for Flight<'_> {
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

impl<'a, 'd, W> policy::Compatible<'d, 'd, W, KeyTag<0>> for Flight<'a>
where
    W: wire::Spec<OpenFailure = failure::Never, TerminalFailure = failure::Never> + 'd,
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

impl<'d> Parser<'d> for Flight<'_> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Flight<'_> {
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

impl<'d> Output<'d> for Flight<'_> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        output::Flush::<_>::Ready(|owner: &mut Self| {
            if let Some(payload) = &owner.payload {
                owner
                    .audit
                    .faults
                    .record(Send(Error::FlushPayload(payload.len())));
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
            if let Some(payload) = &owner.payload {
                owner
                    .audit
                    .faults
                    .record(Send(Error::SentPayload(payload.len())));
            }
        })
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<Infallible, _>::Owner(|(owner, payload): (&mut Self, Self::Send)| {
            if payload.is_empty() || owner.closed.hits() != 0 {
                owner.audit.faults.record(Send(Error::Cancel {
                    bytes: payload.len(),
                    closed: owner.closed.hits() as usize,
                }));
            }
            owner.canceled.hit();
        })
    }
}

impl<'d> session::Connected<'d> for Flight<'_> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        mut context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let Some(payload) = self.payload.take() else {
            self.audit.faults.record(Send(Error::MissingPayload));
            return;
        };
        match context
            .sink
            .try_enqueue(context.driver.region_token(), payload)
        {
            egress::Enqueue::Committed => {}
            egress::Enqueue::Rejected(error) => {
                self.audit.faults.record(error::Error::Enqueue(error))
            }
        }
        if let Some((ready, shutdown)) = self.closing {
            ready.hit();
            if !shutdown {
                context
                    .close
                    .request(close::Directive::permanent(Reason::Local(
                        close::LocalOrigin::ApiRequest,
                    )));
            }
        }
    }
}

impl<'d> session::Failed<'d> for Flight<'_> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Flight<'_> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Flight<'_> {
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
        let expected = self.closing.map(|(_, shutdown)| {
            Reason::Local(if shutdown {
                close::LocalOrigin::RuntimeShutdown
            } else {
                close::LocalOrigin::ApiRequest
            })
        });
        if reason != Reason::Timeout(deadline::Kind::Send) && Some(reason) != expected {
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
        if self.reason.get().is_none() {
            self.reason.set(Some(context.reason()));
        }
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for Flight<'_> {}

impl<'d> Target<'d, KeyTag<0>, 1> for Flight<'_> {}
