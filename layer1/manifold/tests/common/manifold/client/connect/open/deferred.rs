use std::rc::Rc;

use dope_core::{
    driver::{
        self,
        route::Identity,
        schedule::{self, clock},
    },
    io::socket,
};
use dope_manifold::{
    app::receive,
    client::{
        app::{
            self, Application, ChunkOutcome, admit::Automatic, connect, defer, failure, request,
            resume,
        },
        attempt, close, conn,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{open, terminal},
};
use dope_test::harness::signals::{Gate, Progress as _};
use o3::{
    base::cell::region,
    buffer::{bytes::TryRetain, storage::Shared},
    mem::rc,
};

use crate::common::{
    contract::Report,
    manifold::client::{
        connect::{Error, open::DeferredWire},
        error,
    },
};

pub struct State;

pub struct Deferred {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) gate: Gate,
}

impl<'d, ID: Identity> Application<'d, ID> for Deferred {
    type Conn = State;
    type Connect = connect::Full;
    type Deferred = defer::Observed;
    type Failure = failure::Observed;
    type Wire = DeferredWire;
    type Send = Shared;
    type Input = receive::Borrowed;
    type Requests = request::Disabled;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        Ok(State)
    }
}

impl<'d, ID: Identity> app::Receive<'d, ID> for Deferred {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        barrier.ready()
    }
}

impl<'d, ID: Identity> app::Borrowed<'d, ID> for Deferred {
    fn chunk<O, R: TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, DeferredWire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome {
        self.audit.faults.record(Error::Received.into());
        ChunkOutcome::ok()
    }
}

impl<'d, ID: Identity> app::Connected<'d, ID> for Deferred {
    type Activation = Automatic;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, ID, DeferredWire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.gate.hit();
    }
}

impl<'d, ID: Identity> app::Exhausted<'d, ID> for Deferred {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, ID: Identity> app::Failed<'d, ID> for Deferred {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, ID: Identity> app::Opened<'d, 'd, ID> for Deferred {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: open::Error<'d, DeferredWire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'d, ID: Identity> app::Deferred<'d, ID> for Deferred {
    fn deferred(
        &mut self,
        _: attempt::Id<'d, ID>,
        cause: opening::Deferred,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let cause = match cause {
            opening::Deferred::WireBackpressure => return,
            opening::Deferred::Capacity => Error::DeferredCapacity,
            opening::Deferred::Submission(cause) => Error::DeferredSubmit(cause),
        };
        self.audit.faults.record(cause.into());
    }
}

impl<'d, ID: Identity> app::Lifecycle<'d, 'd, ID> for Deferred {
    fn prepare<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, DeferredWire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let ready = self.gate.hits();
        if ready != 1 {
            self.audit.faults.record(Error::Prepared(ready).into());
        }
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
        _: conn::Io<'_, 'd, ID, DeferredWire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        error: terminal::Error<'d, DeferredWire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, DeferredWire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        error: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, DeferredWire, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        let ready = self.gate.hits();
        if ready != 1 {
            self.audit.faults.record(Error::Closed(ready).into());
        }
        app::CloseOutcome::Complete(close::Directive::reconnect(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, ID, Self>,
        _: &mut State,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let ready = self.gate.hits();
        if ready != 1 {
            self.audit.faults.record(Error::Drained(ready).into());
        }
    }
}

impl<'d, ID: Identity> app::Scheduling<'d, ID> for Deferred {
    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
