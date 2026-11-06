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
    depot::queue::pool::fair,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        error::{self, Error::Send},
        send::{Error, wire::Wire},
    },
};

type Request = egress::NonEmpty<Shared>;

pub struct Requests<'d> {
    pub(crate) pending: fair::Lanes<'d, Request, usize>,
    pub(crate) assigned: [Cell<bool>; 4],
    pub(crate) peers: [Option<SocketAddr>; 4],
    pub(crate) next: Rc<Cell<usize>>,
    pub(crate) order: Rc<Trace<usize, 4>>,
    pub(crate) admitted: Rc<Trace<SocketAddr, 4>>,
}

pub struct Lane<'d> {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) requests: Requests<'d>,
    pub(crate) conn: Gate,
    pub(crate) moved: Gate,
    pub(crate) sent: Gate,
}

impl Lane<'_> {
    const REQUESTS: [usize; 4] = [5, 5, 8, 5];
}

impl<'d> Session<'d> for Lane<'d> {
    type Codec = codec::Pending<5>;
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

impl<'d> policy::Compatible<'d, 'd, Wire, KeyTag<0>> for Lane<'d> {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Lane<'d> {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Lane<'d> {
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

impl<'d> Output<'d> for Lane<'d> {
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

impl<'d> Source<'d> for Lane<'d> {
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
        let assigned = &self.requests.assigned[index];
        let mut lane = self.requests.pending.bind(index).expect("request lane");
        if !assigned.get() {
            let position = self.requests.next.get();
            let length = Self::REQUESTS[position];
            let marker = if length == 5 { b'5' } else { b'8' };
            let value =
                Shared::try_from(&mut vec![marker; length]).expect("request storage freeze");
            let value = egress::Load::new(value)
                .try_nonempty()
                .expect("assigned request is nonempty");
            self.requests.next.set(position + 1);
            let permit = lane.reserve().expect("source capacity");
            *permit.state_mut(requests.region()) = length;
            permit.push_back(requests.region(), value);
            assigned.set(true);
        }
        let Some(((value, permit), work)) = requests.claim(&mut lane) else {
            return;
        };
        let length = value.as_inner().len();
        let (value, result) = match work.admit(value) {
            request::Admission::Ready(ready) => {
                ready.push_with(|token| {
                    let charge = permit.state_mut(token);
                    assert_eq!(*charge, length);
                    *charge = 0;
                    drop(permit);
                });
                self.requests.order.record(length);
                self.requests
                    .admitted
                    .record(self.requests.peers[index].expect("connected endpoint"));
                self.moved.hit();
                return;
            }
            request::Admission::Saturated(value) => (value, Ok(())),
            request::Admission::Rejected(rejected) => {
                let (value, cause) = rejected.into_parts();
                (value, Err(cause))
            }
        };
        permit.push_front(requests.region(), value);
        result.expect("source admission");
    }
}

impl<'d> session::Connected<'d> for Lane<'d> {
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

impl<'d> session::Failed<'d> for Lane<'d> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Lane<'d> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Lane<'d> {
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
        if !self.requests.assigned[actual].get() {
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
        let (_, _, token) = context.into_parts();
        if !self.requests.assigned[index].get() {
            self.audit.faults.record(Send(Error::Unassigned(index)));
        }
        let lane = self.requests.pending.bind(index).expect("request lane");
        let view = lane.view(token);
        if let Some(value) = view.front() {
            self.audit.faults.record(Send(Error::Pending {
                index,
                bytes: value.as_inner().len(),
            }));
        }
        assert_eq!(*view.state().value(), 0, "source charge refunded");
    }
}

impl<'d> Scheduling<'d> for Lane<'d> {}

impl<'d> Target<'d, KeyTag<0>, 4> for Lane<'d> {}
