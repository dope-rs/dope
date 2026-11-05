use std::{cell::Cell, convert::Infallible, net::SocketAddr, rc::Rc};

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
    link::{egress, pool::change::opening},
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
        send::{Error, wire::Wire},
    },
};

type Request = egress::NonEmpty<Shared>;

pub struct Requests {
    pub(crate) pending: [(Cell<Option<usize>>, Cell<Option<Request>>); 4],
    pub(crate) peers: [Option<SocketAddr>; 4],
    pub(crate) next: Rc<Cell<usize>>,
    pub(crate) order: Rc<Trace<usize, 4>>,
    pub(crate) admitted: Rc<Trace<SocketAddr, 4>>,
}

pub struct Lane {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) requests: Requests,
    pub(crate) conn: Gate,
    pub(crate) moved: Gate,
    pub(crate) sent: Gate,
}

impl Lane {
    const REQUESTS: [usize; 4] = [5, 5, 8, 5];
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
        let (mut requests, _) = turn.requests();
        let index = requests.connection().index().into_usize();
        let (assigned, pending) = &self.requests.pending[index];
        let length = if let Some(length) = assigned.get() {
            length
        } else {
            let position = self.requests.next.get();
            let length = Self::REQUESTS[position];
            let marker = if length == 5 { b'5' } else { b'8' };
            let value =
                Shared::try_from(&mut vec![marker; length]).expect("request storage freeze");
            let value = egress::Load::new(value)
                .try_nonempty()
                .expect("assigned request is nonempty");
            self.requests.next.set(position + 1);
            pending.set(Some(value));
            assigned.set(Some(length));
            length
        };
        let Some(value) = pending.take() else {
            return;
        };
        match requests.admit(value) {
            request::Admission::Ready(ready) => {
                ready.push();
                self.requests.order.record(length);
                self.requests
                    .admitted
                    .record(self.requests.peers[index].expect("connected endpoint"));
                self.moved.hit();
            }
            request::Admission::Saturated(load) => {
                pending.set(Some(load));
            }
            request::Admission::Exhausted(value) => {
                pending.set(Some(value));
            }
            request::Admission::Rejected(rejected) => {
                self.audit.faults.record(error::Error::Admit(rejected));
            }
        }
    }
}

impl<'d> session::Connected<'d> for Lane {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        peer: socket::Addr,
        context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.requests.peers[context.connection().index().into_usize()] =
            Some(peer.into_std().expect("TCP peer address"));
        self.conn.hit();
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
        conn: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let actual = conn.index().into_usize();
        if self.requests.pending[actual].0.get().is_none() {
            self.audit.faults.record(Send(Error::Unassigned(actual)));
        }
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
        let (assigned, pending) = &mut self.requests.pending[index];
        if assigned.get().is_none() {
            self.audit.faults.record(Send(Error::Unassigned(index)));
        }
        if let Some(value) = pending.get_mut().as_ref() {
            self.audit.faults.record(Send(Error::Pending {
                index,
                bytes: value.as_inner().len(),
            }));
        }
    }
}

impl<'d> Scheduling<'d> for Lane {}

impl<'d> Target<'d, KeyTag<0>, 4> for Lane {}
