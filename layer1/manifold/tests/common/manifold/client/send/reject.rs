use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{driver, driver::route::KeyTag, io::socket};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt,
    close::{Directive, Reason},
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
    link::{egress, egress::reserve::exact, event, pool::change::opening},
    wire::{self, Identity},
};
use dope_test::harness::signals::LocalGateRef;
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        error::{self, Error::Send},
        flush,
        send::Error,
    },
};

pub const MAX: usize = 1;

pub struct Reject<'a, F> {
    pub(crate) create: F,
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) pending: Cell<bool>,
    pub(crate) closed: LocalGateRef<'a>,
    pub(crate) reason: &'a Cell<Option<Reason>>,
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Session<'d> for Reject<'_, F> {
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

impl<'a, 'd, F: Fn() -> egress::NonEmpty<Shared>> policy::Compatible<'d, 'd, Identity, KeyTag<0>>
    for Reject<'a, F>
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

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Parser<'d> for Reject<'_, F> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Plain<'d> for Reject<'_, F> {
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

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Output<'d> for Reject<'_, F> {
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
                    .record(Send(Error::Flush { pending, empty }));
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
        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Source<'d> for Reject<'_, F> {
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
        if !self.pending.get() {
            return;
        }
        let value = (self.create)();
        let outcome = match turn.admit(value) {
            request::Admission::Rejected(rejected) => Ok(rejected),
            request::Admission::Ready(_) => Err(Error::AdmissionReady),
            request::Admission::Exhausted(_) => Err(Error::AdmissionExhausted),
            request::Admission::Saturated(_) => Err(Error::AdmissionSaturated),
        };
        self.pending.set(false);
        let rejected = match outcome {
            Ok(rejected) => rejected,
            Err(cause) => {
                self.audit.faults.record(Send(cause));
                return;
            }
        };
        let (load, cause) = rejected.into_parts();
        let payload = load.into_inner();
        if payload.as_ref() != b"xx" {
            self.audit
                .faults
                .record(Send(Error::Bytes(payload.as_ref().to_vec())));
        }
        drop(payload);
        if !matches!(cause, exact::Failure::Lane { .. }) {
            self.audit
                .faults
                .record(Send(Error::Quarantine(Reason::Egress(cause))));
        }
        turn.close(Directive::cut_retry(Reason::Egress(cause)));
    }
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> session::Connected<'d> for Reject<'_, F> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.pending.set(true);
    }
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> session::Failed<'d> for Reject<'_, F> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> session::Exhausted<'d> for Reject<'_, F> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Retirement<'d> for Reject<'_, F> {
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
        if !matches!(reason, Reason::Egress(exact::Failure::Lane { .. })) {
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

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Scheduling<'d> for Reject<'_, F> {}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Target<'d, KeyTag<0>, MAX> for Reject<'_, F> {}
