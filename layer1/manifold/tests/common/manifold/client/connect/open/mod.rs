use dope_core::driver::{route::Identity, schedule::clock};
use dope_manifold::client::app::resume;
use dope_net::wire::{
    domain, failure, open,
    receive::{Transparent, mode},
    send,
};

use crate::common::{contract::Report, manifold::client::error};

pub mod deferred;
pub mod parked;

use std::{cell::Cell, convert::Infallible, rc::Rc};

use dope_core::{
    driver::{self, schedule},
    io::socket,
};
use dope_manifold::{
    app::receive,
    client::{
        app::{
            self, Application, ChunkOutcome, admit::Automatic, connect::Full, defer::Controlled,
            failure::Observed, request,
        },
        attempt, close, conn,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{self, domain::Limits, terminal},
};
use dope_test::harness::signals::Gate;
use o3::{
    base::cell::region,
    buffer::{bytes::TryRetain, storage::Shared},
    mem::rc,
};

#[derive(Debug, Eq, PartialEq)]
pub enum Cause {
    Permanent,
}

pub struct Wire;

impl failure::Errors<failure::Open> for Wire {
    type Error<'d>
        = Cause
    where
        Self: 'd;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Wire {
    fn open<C, T>(
        &mut self,
        context: C,
        _: impl FnOnce(C, Wire, ()) -> open::Disposition<T, Wire, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        open::Outcome::Failed {
            context,
            cause: open::Error::from_cause(Cause::Permanent),
        }
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Possible;
    type TerminalFailure = failure::Never;

    type Bootstrap = Self;
    type Receive = Transparent<mode::Disabled>;
    type Send = send::Direct;
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: Limits,
        _: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        Wire
    }
}

pub(crate) struct Delay {
    pub(crate) first: Cell<bool>,
    pub(crate) blocked: Cell<bool>,
    pub(crate) opens: Cell<usize>,
    pub(crate) gate: Gate,
}

#[derive(Clone)]
pub struct DeferredWire {
    pub(crate) state: Rc<Delay>,
}

impl DeferredWire {
    pub fn opens(&self) -> usize {
        self.state.opens.get()
    }
}

impl Default for DeferredWire {
    fn default() -> Self {
        Self {
            state: Rc::new(Delay {
                first: Cell::new(true),
                blocked: Cell::new(false),
                opens: Cell::new(0),
                gate: Gate::new(),
            }),
        }
    }
}

pub struct Bootstrap;

impl domain::Init<DeferredWire> for Bootstrap {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: Limits,
        wire: <DeferredWire as wire::Spec>::Binding<'w, ID>,
    ) -> <DeferredWire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        DeferredWire: 'w,
        'd: 'w,
    {
        wire
    }
}

impl wire::Spec for DeferredWire {
    type Connection<'d, ID: Identity + 'd> = wire::Identity;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Self;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;

    type Bootstrap = Bootstrap;
    type Receive = Transparent;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, DeferredWire, ID> for DeferredWire {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, wire::Identity, ()) -> open::Disposition<T, wire::Identity, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, DeferredWire>> {
        self.state.opens.set(self.state.opens.get() + 1);
        if self.state.blocked.get() {
            return open::Outcome::Unavailable(context);
        }
        if self.state.first.replace(false) {
            self.state.gate.hit();
            return open::Outcome::Unavailable(context);
        }
        let output = match consume(context, wire::Identity, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl<ID: Identity> domain::Plan<DeferredWire, ID> for DeferredWire {
    type Error = Infallible;

    fn bind<'d>(self, _: &'d ()) -> Result<Self, Self::Error>
    where
        DeferredWire: 'd,
        ID: 'd,
    {
        Ok(self)
    }
}

pub struct State;

#[derive(Default)]
pub(crate) struct Counts {
    pub(crate) received: Cell<usize>,
    pub(crate) connected: Cell<usize>,
    pub(crate) prepared: Cell<usize>,
    pub(crate) closed: Cell<usize>,
    pub(crate) drained: Cell<usize>,
}

pub struct Failed<F> {
    pub(crate) check: F,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) counts: Rc<Counts>,

    pub(crate) gate: Gate,
    pub(crate) failures: Rc<Cell<u32>>,
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> Application<'d, ID>
    for Failed<F>
{
    type Conn = State;
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = Wire;
    type Send = Shared;
    type Input = receive::Borrowed;
    type Requests = request::Disabled;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        Ok(State)
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Receive<'d, ID>
    for Failed<F>
{
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        barrier.ready()
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Borrowed<'d, ID>
    for Failed<F>
{
    fn chunk<O, R: TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome {
        self.counts.received.set(self.counts.received.get() + 1);
        ChunkOutcome::ok()
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Connected<'d, ID>
    for Failed<F>
{
    type Activation = Automatic;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.counts.connected.set(self.counts.connected.get() + 1);
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Exhausted<'d, ID>
    for Failed<F>
{
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        (self.check)(cause.into());
        self.failures.set(self.failures.get() + 1);
        self.gate.hit();
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Failed<'d, ID>
    for Failed<F>
{
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Opened<'d, 'd, ID>
    for Failed<F>
{
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: open::Error<'d, Wire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        (self.check)(opening::Failure::Wire(cause));
        self.failures.set(self.failures.get() + 1);
        self.gate.hit();
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Lifecycle<'d, 'd, ID>
    for Failed<F>
{
    fn prepare<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.counts.prepared.set(self.counts.prepared.get() + 1);
    }

    fn sent(
        &mut self,
        _: conn::Id<'d, ID>,
        _: egress::Pending,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.audit.record_sent();
    }

    fn cancel(&mut self, _: Shared, _: &mut region::Token<region::Identity<'d, 1>>) {
        self.audit.cancelled();
    }

    fn wire_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        error: terminal::Error<'d, Wire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        error: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        self.counts.closed.set(self.counts.closed.get() + 1);
        app::CloseOutcome::Complete(close::Directive::cut_final(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, ID, Self>,
        _: &mut State,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.counts.drained.set(self.counts.drained.get() + 1);
    }
}

impl<'d, ID: Identity, F: Fn(opening::Failure<open::Error<'d, Wire>>)> app::Scheduling<'d, ID>
    for Failed<F>
{
    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
