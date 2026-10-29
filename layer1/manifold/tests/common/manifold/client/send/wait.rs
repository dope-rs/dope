use std::{
    cell::{Cell, RefCell},
    convert::Infallible,
    rc::Rc,
};

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
    link::{egress, event, pool::change::opening},
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
        flush,
        send::{Error, wire::Wire},
    },
};

pub const MAX: usize = 1;
pub const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

pub struct Wait<F> {
    pub(crate) create: F,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) pending: RefCell<[Option<egress::NonEmpty<Shared>>; 2]>,
    pub(crate) index: Rc<Cell<Option<usize>>>,
    pub(crate) reasons: Rc<Trace<(usize, Reason), 2>>,
    pub(crate) seen: Gate,
    pub(crate) closed: Gate,
}
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Session<'d> for Wait<F> {
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

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> policy::Compatible<'d, 'd, Wire, KeyTag<0>>
    for Wait<F>
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

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Parser<'d> for Wait<F> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Plain<'d> for Wait<F> {
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
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Output<'d> for Wait<F> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let actual = context.conn_id.index().into_usize();
            let capacity = owner.pending.get_mut().len();
            if actual >= capacity {
                owner
                    .audit
                    .faults
                    .record(Send(Error::Index { actual, capacity }));
            }
        })
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible, Infallible, Infallible, _>::Queue(
            |(owner, conn, _): (&mut Self, conn::Id<'d, KeyTag<0>>, egress::Pending)| {
                let actual = conn.index().into_usize();
                let capacity = owner.pending.get_mut().len();
                if actual >= capacity {
                    owner
                        .audit
                        .faults
                        .record(Send(Error::Index { actual, capacity }));
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
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Source<'d> for Wait<F> {
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
        let mut pending = match self.pending.try_borrow_mut() {
            Ok(pending) => pending,
            Err(error) => {
                self.audit.faults.record(Send(Error::BorrowMut(error)));
                return;
            }
        };
        let capacity = pending.len();
        let Some(slot) = pending.get_mut(index) else {
            self.audit.faults.record(Send(Error::Index {
                actual: index,
                capacity,
            }));
            return;
        };
        let Some(value) = slot.take() else {
            return;
        };
        match requests.admit(value) {
            request::Admission::Ready(ready) => ready.push(),
            request::Admission::Saturated(load) => {
                *slot = Some(load);
                if self.index.get().is_none() {
                    self.index.set(Some(index));
                    self.seen.hit();
                }
            }
            request::Admission::Exhausted(value) => {
                *slot = Some(value);
            }
            request::Admission::Rejected(rejected) => {
                self.audit.faults.record(error::Error::Admit(rejected));
            }
        }
    }
}
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> session::Connected<'d> for Wait<F> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let index = context.connection().index().into_usize();
        let pending = self.pending.get_mut();
        let capacity = pending.len();
        let Some(slot) = pending.get_mut(index) else {
            self.audit.faults.record(Send(Error::Index {
                actual: index,
                capacity,
            }));
            return;
        };
        *slot = Some((self.create)());
    }
}
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> session::Failed<'d> for Wait<F> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> session::Exhausted<'d> for Wait<F> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Retirement<'d> for Wait<F> {
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
impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Scheduling<'d> for Wait<F> {}

impl<'d, F: Fn() -> egress::NonEmpty<Shared>> Target<'d, KeyTag<0>, 2> for Wait<F> {}
