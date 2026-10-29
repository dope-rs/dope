use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{
    driver,
    driver::route::{Identity, KeyTag},
    io::{recv, socket},
    platform::wake,
};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt, close, conn,
    session::{
        self, Output, Parser, Plain, Retirement, Scheduling, Session, Target,
        app::drain::Ready,
        codec::mode,
        policy::{self, end, output},
        response, turns,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{
        self, domain, failure, open, receive,
        receive::{batch, eof, mode::Disabled},
        send, terminal,
    },
};
use dope_test::harness::{observe::Trace, signals};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};
use wire::receive::{mode::access, reserve};

use crate::common::{
    contract::codec::{Pending, errors},
    manifold::client::flush,
};

pub mod outgoing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Receive,
    Close,
    Completion,
}

pub struct Fault;

pub struct State {
    pub(crate) failed: bool,
}

pub struct Ingress;

impl receive::Policy<Fault> for Ingress {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = Disabled;
    type Discard = Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Fault>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Fault>,
        end: terminal::Both<'_, terminal::Error<'w, Fault>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Fault: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let output = capacity.one(&*bytes);
        let wire = txn.into_wire();
        if wire.failed {
            return end.output(output);
        }
        wire.failed = true;
        let failure = terminal::Terminal::Failed(terminal::Error::from_cause(Cause::Receive));
        end.terminal(output, failure)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Fault>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Fault>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Fault: 'w,
        'd: 'w,
        'w: 'a,
    {
        let output = Some(bytes.into_shared());
        let wire = txn.into_wire();
        if wire.failed {
            return end.output(output);
        }
        wire.failed = true;
        let failure = terminal::Terminal::Failed(terminal::Error::from_cause(Cause::Receive));
        end.terminal(output, failure)
    }
}

impl eof::Eof<Fault> for Ingress {
    fn eof<'w, ID: Identity + 'w>(
        state: &mut State,
        end: terminal::Send<'_, terminal::Error<'w, Fault>>,
    ) -> terminal::Processed<()>
    where
        Fault: 'w,
    {
        state.failed = true;
        let failure = terminal::Terminal::Failed(terminal::Error::from_cause(Cause::Receive));
        end.terminal((), failure)
    }
}

impl failure::Errors<failure::Terminal> for Fault {
    type Error<'d> = Cause;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Fault, ID> for Fault {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, State, ()) -> open::Disposition<T, State, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Fault>> {
        let output = match consume(context, State { failed: false }, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl wire::Spec for Fault {
    type Connection<'d, ID: Identity + 'd> = State;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Self;
    type Receive = Ingress;
    type Send = send::Direct;
}

impl domain::Init<Fault> for Fault {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        _: <Self as wire::Spec>::Binding<'w, ID>,
    ) -> <Self as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Self: 'w,
        'd: 'w,
    {
        Self
    }
}

pub struct Failure {
    pub(crate) release: Option<wake::Notify>,
    pub(crate) events: Rc<Trace<Event, 4>>,
    pub(crate) conn: signals::Gate,
    pub(crate) recv_drained: Cell<bool>,
    pub(crate) closed: signals::Gate,
}

#[derive(Debug)]
pub enum Event {
    Connected {
        applied: Option<close::Directive>,
        notified: Option<Result<(), wake::NotifyError>>,
    },
    Quarantined,
    Failed(Cause, Option<Cause>),
    Closed(close::Reason),
    Sent,
    Cancelled,
    Flush {
        released: bool,
        empty: bool,
    },
    ConnectFault(event::ConnectFault),
    Exhausted(opening::Exhaustion),
    Receive(dope_core::Errno),
}

impl<'d> Session<'d> for Failure {
    type Codec = Pending;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Send = Shared;
    type Requests = request::Disabled;

    fn codec(&self) -> &Self::Codec {
        &Pending
    }
}

impl<'d> Parser<'d> for Failure {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Failure {
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

impl<'d> Output<'d> for Failure {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let released = owner.release.is_none();
            let empty = context.sink.is_empty();
            if !released || !empty {
                owner.events.record(Event::Flush { released, empty });
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
            owner.events.record(Event::Sent);
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
                owner.events.record(Event::Cancelled);
            },
        )
    }
}

impl<'d, W> end::Sink<'d, 'd, W, KeyTag<0>, failure::Possible> for Failure
where
    W: wire::Spec<TerminalFailure = failure::Possible>
        + failure::Errors<failure::Terminal, Error<'d> = Cause>
        + 'd,
{
    fn fail(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        cause: terminal::Error<'d, W>,
        _: &mut region::Token<region::Identity<'d>>,
    ) {
        let (cause, closing) = cause.into_parts();
        self.events.record(Event::Failed(cause, closing));
        self.recv_drained.set(true);
    }
}

impl<'d, W> policy::Compatible<'d, 'd, W, KeyTag<0>> for Failure
where
    W: wire::Spec<OpenFailure = failure::Never, TerminalFailure = failure::Possible>
        + failure::Errors<failure::Terminal, Error<'d> = Cause>
        + 'd,
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

impl<'d> session::Connected<'d> for Failure {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        mut context: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        let rule =
            close::Directive::permanent(close::Reason::Local(close::LocalOrigin::ApiRequest));
        context.close.request(rule);
        let applied = match context.close.status() {
            conn::CloseStatus::Open => None,
            conn::CloseStatus::Closing(_, applied) => Some(applied),
        };
        let notified = self.release.take().map(|release| release.notify());
        self.events.record(Event::Connected { applied, notified });
        self.conn.hit();
    }
}

impl<'d> session::Failed<'d> for Failure {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.events.record(Event::ConnectFault(error));
    }
}

impl<'d> session::Exhausted<'d> for Failure {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.events.record(Event::Exhausted(error));
    }
}

impl<'d> Retirement<'d> for Failure {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        error: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.events.record(Event::Receive(error));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        self.events.record(Event::Quarantined);
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn clear_recv<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: close::Reason,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        if self.recv_drained.get() {
            egress::Cleared::Done
        } else {
            egress::Cleared::Waiting
        }
    }

    fn disconnect(&mut self, context: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.events.record(Event::Closed(context.reason()));
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for Failure {}
impl<'d> Target<'d, KeyTag<0>, 1> for Failure {}
