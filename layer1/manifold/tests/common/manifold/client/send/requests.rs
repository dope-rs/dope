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
    wire,
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

type Request = egress::NonEmpty<Shared>;

pub struct Requests<F> {
    pub(crate) create: F,
    pub(crate) pending: RefCell<[(Option<usize>, Option<Request>); 4]>,
    pub(crate) next: Rc<Cell<usize>>,
    pub(crate) order: Rc<Trace<usize, 4>>,
    pub(crate) admitted: Rc<Trace<usize, 4>>,
}

pub struct Lane<F> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) requests: Requests<F>,
    pub(crate) conn: Gate,
    pub(crate) moved: Gate,
    pub(crate) sent: Gate,
}

impl<F> Lane<F> {
    const REQUESTS: [usize; 4] = [5, 5, 8, 5];
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Session<'d> for Lane<F> {
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

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> policy::Compatible<'d, 'd, Wire, KeyTag<0>>
    for Lane<F>
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

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Parser<'d> for Lane<F> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Plain<'d> for Lane<F> {
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

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Output<'d> for Lane<F> {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let actual = context.conn_id.index().into_usize();
            let capacity = owner.requests.pending.get_mut().len();
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
            |(owner, conn, pending): (&mut Self, conn::Id<'d, KeyTag<0>>, egress::Pending)| {
                let actual = conn.index().into_usize();
                let capacity = owner.requests.pending.get_mut().len();
                if actual >= capacity {
                    owner
                        .audit
                        .faults
                        .record(Send(Error::Index { actual, capacity }));
                }
                match pending {
                    egress::Pending::Absent => owner.sent.hit(),
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
        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Source<'d> for Lane<F> {
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
        let mut slots = match self.requests.pending.try_borrow_mut() {
            Ok(slots) => slots,
            Err(error) => {
                self.audit.faults.record(Send(Error::BorrowMut(error)));
                return;
            }
        };
        let capacity = slots.len();
        let Some((assigned, pending)) = slots.get_mut(index) else {
            self.audit.faults.record(Send(Error::Index {
                actual: index,
                capacity,
            }));
            return;
        };
        let length = if let Some(length) = *assigned {
            length
        } else {
            let position = self.requests.next.get();
            let (length, value) = (self.requests.create)(Self::REQUESTS.get(position).copied());
            self.requests.next.set(position + 1);
            *pending = Some(value);
            *assigned = Some(length);
            length
        };
        let Some(value) = pending.take() else {
            return;
        };
        match requests.admit(value) {
            request::Admission::Ready(ready) => {
                ready.push();
                self.requests.order.record(length);
                self.requests.admitted.record(index);
                self.moved.hit();
            }
            request::Admission::Saturated(load) => {
                *pending = Some(load);
            }
            request::Admission::Exhausted(value) => {
                *pending = Some(value);
            }
            request::Admission::Rejected(rejected) => {
                self.audit.faults.record(error::Error::Admit(rejected));
            }
        }
    }
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> session::Connected<'d> for Lane<F> {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.conn.hit();
    }
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> session::Failed<'d> for Lane<F> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> session::Exhausted<'d> for Lane<F> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Retirement<'d> for Lane<F> {
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
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let actual = conn.index().into_usize();
        let slots = match self.requests.pending.try_borrow() {
            Ok(slots) => slots,
            Err(error) => {
                self.audit.faults.record(Send(Error::Borrow(error)));
                return;
            }
        };
        let cause = match slots.get(actual) {
            Some((Some(_), _)) => return,
            Some((None, _)) => Error::Unassigned(actual),
            None => Error::Index {
                actual,
                capacity: slots.len(),
            },
        };
        self.audit.faults.record(Send(cause));
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, (), KeyTag<0>, Self>) {
        let index = context.connection().index().into_usize();
        let slots = self.requests.pending.get_mut();
        let Some((assigned, pending)) = slots.get(index) else {
            self.audit.faults.record(Send(Error::Index {
                actual: index,
                capacity: slots.len(),
            }));
            return;
        };
        if assigned.is_none() {
            self.audit.faults.record(Send(Error::Unassigned(index)));
        }
        if let Some(value) = pending {
            self.audit.faults.record(Send(Error::Pending {
                index,
                bytes: value.as_inner().len(),
            }));
        }
    }
}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Scheduling<'d> for Lane<F> {}

impl<'d, F: Fn(Option<usize>) -> (usize, Request)> Target<'d, KeyTag<0>, 4> for Lane<F> {}
