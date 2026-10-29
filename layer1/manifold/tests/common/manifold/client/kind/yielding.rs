use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{
        self, route,
        schedule::{self, clock},
    },
    io::socket::{self},
};
use dope_manifold::{
    app::receive,
    client::{
        app::{
            self, Application, ChunkOutcome, admit::Automatic, connect::Full, defer::Controlled,
            failure::Observed, request, resume,
        },
        attempt,
        close::{self, Directive, LocalOrigin, Reason},
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

pub struct Life {
    pub(crate) conn: Gate,
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
}

pub struct Yielding {
    pub(crate) audit: Rc<Report<error::Error, 16>>,

    pub(crate) life: Life,
    pub(crate) pending: Cell<bool>,
    pub(crate) yielded: Cell<bool>,
    pub(crate) checks: Rc<Cell<usize>>,
    pub(crate) resumed: Rc<Cell<bool>>,
}

impl<'d, ID: route::Identity> Application<'d, ID> for Yielding {
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

impl<'d, ID: route::Identity> app::Receive<'d, ID> for Yielding {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        let checks = self.checks.get() + 1;
        self.checks.set(checks);
        if checks > 1 {
            barrier.ready()
        } else {
            barrier.pending()
        }
    }
}

impl<'d, ID: route::Identity> app::Borrowed<'d, ID> for Yielding {
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

impl<'d, ID: route::Identity> app::Connected<'d, ID> for Yielding {
    type Activation = Automatic;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.life.conn.hit();
        self.pending.set(true);
    }
}

impl<'d, ID: route::Identity> app::Exhausted<'d, ID> for Yielding {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, ID: route::Identity> app::Failed<'d, ID> for Yielding {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, ID: route::Identity> app::Opened<'d, 'd, ID> for Yielding {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: open::Error<'d, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'d, ID: route::Identity> app::Lifecycle<'d, 'd, ID> for Yielding {
    fn prepare<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Identity, State, O>,
        _: egress::Queue<'_, '_, 'd, 32>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let conns = self.life.conn.hits();
        if conns != 1 {
            self.audit
                .faults
                .record(Kind(Error::PrepareConnections(conns)));
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
        let conns = self.life.conn.hits();
        if conns != 1 {
            self.audit
                .faults
                .record(Kind(Error::CloseConnections(conns)));
        }
        if !self.yielded.replace(true) {
            app::CloseOutcome::Yield
        } else {
            self.resumed.set(true);
            self.life.closed.hit();
            app::CloseOutcome::Complete(Directive::reconnect(reason))
        }
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, ID, Self>,
        _: &mut State,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.life.retired.hit();
    }
}

impl<'d, ID: route::Identity> app::Requests<'d, ID> for Yielding {
    fn drain(&self, turn: &mut request::Turn<'_, '_, 'd, State, Shared, ID>) {
        if self.pending.replace(false) {
            turn.close(Directive::cut_final(Reason::Local(
                LocalOrigin::ResourceFailure,
            )));
        }
    }
}

impl<'d, ID: route::Identity> app::Scheduling<'d, ID> for Yielding {
    fn shutdown() -> Option<impl for<'a> Call<&'a mut Self>> {
        Some(|this: &mut Self| {
            let closed = this.life.closed.hits();
            let retired = this.life.retired.hits();
            if (closed, retired) != (1, 1) {
                this.audit
                    .faults
                    .record(Kind(Error::YieldingShutdown { closed, retired }));
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
