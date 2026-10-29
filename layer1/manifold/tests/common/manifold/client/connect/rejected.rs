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
            self, Application, admit, connect::Full, defer::Controlled, failure::Observed, request,
            resume,
        },
        attempt, close, conn,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{Identity, open, terminal},
};
use dope_test::harness::signals::Gate;
use o3::{
    base::cell::region,
    buffer::{bytes::TryRetain, storage::Shared},
    mem::rc,
};

use crate::common::{contract::Report, manifold::client::error};

pub struct Rejected {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) gate: Gate,
    pub(crate) scoped: Rc<Cell<u32>>,
}

impl Rejected {
    pub(crate) fn mark(&self) {
        self.scoped.set(self.scoped.get() + 1);
    }
}

pub struct State;

impl<'d, ID: route::Identity> Application<'d, ID> for Rejected {
    type Conn = State;
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = Identity;
    type Send = Shared;
    type Input = receive::Borrowed;
    type Requests = request::Disabled;

    fn try_conn(&self) -> Result<Self::Conn, rc::Error> {
        Ok(State)
    }
}

impl<'d, ID: route::Identity> app::Receive<'d, ID> for Rejected {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        barrier.ready()
    }
}

impl<'d, ID: route::Identity> app::Borrowed<'d, ID> for Rejected {
    fn chunk<O, R: TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::ChunkOutcome {
        self.mark();
        app::ChunkOutcome::ok()
    }
}

impl admit::Check for Rejected {
    fn admit(&mut self) -> Result<(), close::Directive> {
        self.gate.hit();
        Err(close::Directive::cut_final(close::Reason::Local(
            close::LocalOrigin::ResourceFailure,
        )))
    }
}

impl<'d, ID: route::Identity> app::Connected<'d, ID> for Rejected {
    type Activation = admit::Checked;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.mark();
    }
}

impl<'d, ID: route::Identity> app::Exhausted<'d, ID> for Rejected {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, ID: route::Identity> app::Failed<'d, ID> for Rejected {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error))
    }
}

impl<'d, ID: route::Identity> app::Opened<'d, 'd, ID> for Rejected {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: open::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'d, ID: route::Identity> app::Lifecycle<'d, 'd, ID> for Rejected {
    fn prepare<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.mark();
    }

    fn sent(
        &mut self,
        _: conn::Id<'d, ID>,
        _: egress::Pending,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.mark();
    }

    fn cancel(&mut self, _: Shared, _: &mut region::Token<region::Identity<'d, 1>>) {
        self.mark();
    }

    fn wire_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: terminal::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.mark();
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        _: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.mark();
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32, Shared>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        self.mark();
        app::CloseOutcome::Complete(close::Directive::permanent(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, ID, Self>,
        _: &mut State,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.mark();
    }
}

impl<'d, ID: route::Identity> app::Scheduling<'d, ID> for Rejected {
    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
