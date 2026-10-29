use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{
        self,
        route::KeyTag,
        schedule::{self, clock},
    },
    io::socket,
};
use dope_manifold::{
    app::receive,
    client::{
        app::{
            self, Application, ChunkOutcome, CloseOutcome,
            admit::{Register, Registered},
            connect::Full,
            defer::Controlled,
            failure::Observed,
            request, resume,
        },
        attempt, close, conn,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{Identity, open, terminal},
};
use dope_test::harness::{observe, signals::Gate};
use o3::{
    base::cell::region,
    buffer::{bytes::TryRetain, storage::Shared},
    mem::rc,
};

use crate::common::{contract::Report, manifold::client::error};

pub struct Empty {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) trace: Rc<Trace>,
    pub(crate) reject: Option<Gate>,
}

#[derive(Default)]
pub struct Trace {
    pub(crate) admitted: Cell<usize>,
    pub(crate) connected: Cell<usize>,
    pub(crate) failed: observe::Trace<event::ConnectFault, 16>,
    pub(crate) prepared: Cell<usize>,
    pub(crate) closed: Cell<usize>,
    pub(crate) drained: Cell<usize>,
    pub(crate) reason: Cell<Option<close::Reason>>,
}

impl<'d> Application<'d> for Empty {
    type Conn = ();
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = Identity;
    type Send = Shared;
    type Input = receive::Borrowed;
    type Requests = request::Disabled;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        if self.reject.is_some() {
            return Err(rc::Error::Exhausted);
        }
        Ok(())
    }
}

impl<'d> app::Receive<'d> for Empty {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, KeyTag<0>>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, KeyTag<0>> {
        barrier.ready()
    }
}

impl<'d> app::Borrowed<'d> for Empty {
    fn chunk<O, R: TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome {
        ChunkOutcome::ok()
    }
}

impl<'d> app::Connected<'d> for Empty {
    type Activation = Registered;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.trace.connected.set(self.trace.connected.get() + 1);
    }
}
impl<'d> Register<'d> for Empty {
    fn register(&mut self, _: &conn::Activation<'_, 'd, KeyTag<0>>) -> app::Admission {
        self.trace.admitted.set(self.trace.admitted.get() + 1);
        app::Admission::Attached
    }
}
impl<'d> app::Exhausted<'d> for Empty {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d> app::Failed<'d> for Empty {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd>,
        cause: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.trace.failed.record(cause);
        if let Some(gate) = &self.reject {
            gate.hit();
        }
    }
}
impl<'d> app::Opened<'d, 'd> for Empty {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd>,
        error: open::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}
impl<'d> app::Lifecycle<'d, 'd> for Empty {
    fn prepare<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.trace.prepared.set(self.trace.prepared.get() + 1);
    }

    fn sent(
        &mut self,
        _: conn::Id<'d, KeyTag<0>>,
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
        _: conn::Io<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        error: terminal::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        error: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> CloseOutcome {
        self.trace.closed.set(self.trace.closed.get() + 1);
        if self.trace.reason.get().is_none() {
            self.trace.reason.set(Some(reason));
        }
        CloseOutcome::Complete(close::Directive::reconnect(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, KeyTag<0>, Self>,
        _: &mut Self::Conn,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.trace.drained.set(self.trace.drained.get() + 1);
    }
}
impl<'d> app::Scheduling<'d> for Empty {
    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
