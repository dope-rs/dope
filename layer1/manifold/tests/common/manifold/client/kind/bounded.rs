use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{
        self, route,
        schedule::{self, clock},
    },
    io::socket,
};
use dope_manifold::{
    app::receive,
    client::{
        app::{
            self, Application, ChunkOutcome, admit::Automatic, connect::Full, defer::Controlled,
            failure::Observed, request, resume,
        },
        attempt,
        close::{self, Directive},
        conn,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{Identity, open, terminal},
};
use dope_test::harness::signals::{Gate, Progress as _};
use o3::{
    base::{Call, cell::region},
    buffer::{bytes::TryRetain, storage::Shared},
    mem::rc,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        error::{self, Error::Kind},
        kind::Error,
    },
};

pub struct State;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Phase {
    New,
    Live,
    Closing,
    Drained,
}

pub struct Bounded<F> {
    pub(crate) create: F,
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) done: Gate,
    pub(crate) left: Cell<usize>,
    pub(crate) calls: Rc<Cell<usize>>,
    pub(crate) first: Rc<Cell<Option<usize>>>,
    pub(crate) phase: Cell<Phase>,
}

impl<F> Bounded<F> {
    fn active(&self) {
        let phase = self.phase.get();
        if !matches!(phase, Phase::Live | Phase::Closing) {
            self.audit.faults.record(Kind(Error::Active(phase)));
        }
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> Application<'d, ID>
    for Bounded<F>
{
    type Conn = State;
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = Identity;
    type Send = Shared;
    type Input = receive::Borrowed;
    type Requests = request::Enabled;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        Ok(State)
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Receive<'d, ID>
    for Bounded<F>
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

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Borrowed<'d, ID>
    for Bounded<F>
{
    fn chunk<O, R: TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome {
        self.audit.faults.record(Kind(Error::Received));
        ChunkOutcome::ok()
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Connected<'d, ID>
    for Bounded<F>
{
    type Activation = Automatic;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let phase = self.phase.replace(Phase::Live);
        if phase != Phase::New {
            self.audit.faults.record(Kind(Error::Connected(phase)));
        }
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Exhausted<'d, ID>
    for Bounded<F>
{
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Failed<'d, ID>
    for Bounded<F>
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

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Opened<'d, 'd, ID>
    for Bounded<F>
{
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: open::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Lifecycle<'d, 'd, ID>
    for Bounded<F>
{
    fn prepare<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.active();
    }

    fn sent(
        &mut self,
        _: conn::Id<'d, ID>,
        _: egress::Pending,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.active();
    }

    fn cancel(&mut self, payload: Shared, _: &mut region::Token<region::Identity<'d, 1>>) {
        if payload.as_ref() != b"x" {
            self.audit
                .faults
                .record(Kind(Error::Bytes(payload.as_ref().to_vec())));
        }
        self.active();
    }

    fn wire_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        error: terminal::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        error: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        let phase = self.phase.replace(Phase::Closing);
        if phase != Phase::Live {
            self.audit.faults.record(Kind(Error::Closing(phase)));
        }
        app::CloseOutcome::Complete(Directive::reconnect(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, ID, Self>,
        _: &mut State,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let phase = self.phase.replace(Phase::Drained);
        if phase != Phase::Closing {
            self.audit.faults.record(Kind(Error::Drained(phase)));
        }
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Requests<'d, ID>
    for Bounded<F>
{
    fn drain(&self, turn: &mut request::Turn<'_, '_, 'd, State, Shared, ID>) {
        let call = self.calls.get();
        self.calls.set(call + 1);
        while self.left.get() != 0 {
            let value = (self.create)();
            let ready = match turn.admit(value) {
                request::Admission::Ready(ready) => ready,
                request::Admission::Exhausted(value) | request::Admission::Saturated(value) => {
                    if value.as_inner().as_ref() != b"x" {
                        self.audit
                            .faults
                            .record(Kind(Error::Bytes(value.as_inner().as_ref().to_vec())));
                    }
                    break;
                }
                request::Admission::Rejected(rejected) => {
                    self.audit.faults.record(error::Error::Admit(rejected));
                    return;
                }
            };
            ready.push();
            let left = self.left.get() - 1;
            self.left.set(left);
            if left == 0 {
                self.done.hit();
            }
        }
        if call == 0 {
            self.first.set(Some(self.left.get()));
        }
    }
}

impl<'d, ID: route::Identity, F: Fn() -> egress::NonEmpty<Shared>> app::Scheduling<'d, ID>
    for Bounded<F>
{
    fn shutdown() -> Option<impl for<'a> Call<&'a mut Self>> {
        Some(|this: &mut Self| {
            let left = this.left.get();
            let done = this.done.hits();
            if (left, done) != (0, 1) {
                this.audit
                    .faults
                    .record(Kind(Error::BoundedShutdown { left, done }));
            }
        })
    }

    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
