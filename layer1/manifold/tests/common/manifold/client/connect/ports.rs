use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{
        self,
        route::KeyTag,
        schedule::{self, app::Work, clock},
    },
    io::socket,
};
use dope_manifold::{
    app::receive,
    client::{
        app::{self, admit, connect::Full, defer::Controlled, failure::Observed, request, resume},
        attempt, close, conn,
        port::{Port, error::Open},
    },
};
use dope_net::{
    link::{egress, pool::change::opening},
    wire::{open, terminal},
};
use dope_test::harness::signals::Gate;
use o3::{
    base::{Call, cell::region},
    buffer::{bytes::TryRetain, pool, write::consume},
    mem::rc,
};

use crate::common::{
    contract::Report,
    manifold::client::{connect::Error, error, send::wire::Wire},
};

pub struct App<'e, 'd, F> {
    pub(crate) ports: Vec<Port<'e, 'd, pool::Cursor>>,
    pub(crate) call: Option<F>,
    pub(crate) done: Gate,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) closed: Gate,
    pub(crate) reason: Rc<Cell<Option<close::Reason>>>,
}

pub const PAYLOAD: &[u8] = b"payload";
pub const NESTED: &[u8] = b"nested";

pub fn cursor(buffers: &pool::Pool, bytes: &[u8]) -> Result<Option<pool::Cursor>, consume::Error> {
    let Some(mut cursor) = buffers.cursor() else {
        return Ok(None);
    };
    cursor.try_append(bytes)?.commit();
    Ok(Some(cursor))
}

impl<'e, 'd, F> App<'e, 'd, F>
where
    F: for<'turn, 'queue> FnOnce(
        &[Port<'e, 'd, pool::Cursor>],
        conn::Id<'d, KeyTag<0>>,
        Work<'turn, 'd>,
        egress::Queue<'_, 'queue, 'd, 32, pool::Cursor>,
        &mut region::Token<region::Identity<'d>>,
    ) -> Option<close::Directive>,
{
    pub fn new(
        ports: Vec<Port<'e, 'd, pool::Cursor>>,
        done: Gate,
        audit: Rc<Report<error::Error, 16>>,
        call: F,
    ) -> Self {
        Self {
            ports,
            call: Some(call),
            done,
            audit,
            closed: Gate::new(),
            reason: Rc::new(Cell::new(None)),
        }
    }
}

impl<'d, F> app::Application<'d> for App<'_, 'd, F> {
    type Conn = ();
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = Wire;
    type Send = pool::Cursor;
    type Input = receive::Borrowed;
    type Requests = request::Enabled;

    fn try_conn(&self) -> Result<(), rc::Error> {
        Ok(())
    }
}

impl<'d, F> app::Receive<'d> for App<'_, 'd, F> {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, KeyTag<0>>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, KeyTag<0>> {
        barrier.ready()
    }
}

impl<'d, F> app::Borrowed<'d> for App<'_, 'd, F> {
    fn chunk<O, R: TryRetain>(
        &mut self,
        conn: conn::Ctx<'_, 'd, KeyTag<0>, Wire, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, pool::Cursor>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::ChunkOutcome {
        for port in &self.ports {
            port.close(
                conn.id(),
                close::Directive::cut_retry(close::Reason::Timeout(
                    dope_manifold::client::deadline::Kind::Inbound,
                )),
            );
        }
        app::ChunkOutcome::ok()
    }
}

impl<'d, F> admit::Register<'d> for App<'_, 'd, F> {
    fn register(&mut self, active: &conn::Activation<'_, 'd, KeyTag<0>>) -> app::Admission {
        for port in &self.ports {
            let first = port.try_activate(active);
            let second = port.try_activate(active);
            if first.is_err() || second != Err(Open::Occupied) {
                self.audit
                    .faults
                    .record(Error::Activation { first, second }.into());
            }
        }
        app::Admission::Attached
    }
}

impl<'d, F> app::Connected<'d> for App<'_, 'd, F> {
    type Activation = admit::Registered;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        conn: conn::Established<'_, 'd, KeyTag<0>, Wire, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, pool::Cursor>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        if let Some(index) = self
            .ports
            .iter()
            .position(|port| !port.is_active(conn.id()))
        {
            self.audit.faults.record(Error::InactivePort(index).into());
        }
    }
}

impl<'d, F> app::Exhausted<'d> for App<'_, 'd, F> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, F> app::Failed<'d> for App<'_, 'd, F> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(cause));
    }
}

impl<'d, F> app::Opened<'d, 'd> for App<'_, 'd, F> {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: open::Error<'d, Wire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'e, 'd, F> app::Lifecycle<'d, 'd> for App<'e, 'd, F>
where
    F: for<'turn, 'queue> FnOnce(
        &[Port<'e, 'd, pool::Cursor>],
        conn::Id<'d, KeyTag<0>>,
        Work<'turn, 'd>,
        egress::Queue<'_, 'queue, 'd, 32, pool::Cursor>,
        &mut region::Token<region::Identity<'d>>,
    ) -> Option<close::Directive>,
{
    fn take_close(&self, conn: conn::Id<'d, KeyTag<0>>) -> Option<close::Directive> {
        self.ports.iter().find_map(|port| port.take_close(conn))
    }

    fn prepare<O>(
        &mut self,
        mut context: conn::Ctx<'_, 'd, KeyTag<0>, Wire, (), O>,
        _: bool,
        queue: egress::Queue<'_, '_, 'd, 32, pool::Cursor>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if let Some(call) = self.call.take() {
            if let Some(rule) = call(
                &self.ports,
                context.id(),
                context.work(),
                queue,
                driver.region_token(),
            ) {
                context.request(rule);
            }
            self.done.hit();
        }
    }

    fn sent(
        &mut self,
        _: conn::Id<'d, KeyTag<0>>,
        _: egress::Pending,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.audit.record_sent();
    }

    fn cancel(&mut self, _: pool::Cursor, _: &mut region::Token<region::Identity<'d, 1>>) {
        self.audit.cancelled();
    }

    fn wire_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, Wire, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, pool::Cursor>,
        cause: terminal::Error<'d, Wire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        cause.eliminate()
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, Wire, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, pool::Cursor>,
        cause: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(cause));
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Wire, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, pool::Cursor>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        app::CloseOutcome::Complete(close::Directive::permanent(reason))
    }

    fn drained(
        &mut self,
        proof: conn::Retired<'d, KeyTag<0>, Self>,
        _: &mut (),
        _: &mut driver::Context<'_, 'd>,
    ) {
        if self.call.is_some() {
            self.audit.faults.record(Error::PendingCall.into());
        }
        self.reason.set(Some(proof.reason()));
        self.closed.hit();
    }
}

impl<'d, F> app::Scheduling<'d> for App<'_, 'd, F> {
    fn shutdown() -> Option<impl for<'a> Call<&'a mut Self>> {
        Some(|this: &mut Self| {
            if this.call.is_some() {
                this.audit.faults.record(Error::PendingCall.into());
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

impl<'d, F> app::Requests<'d> for App<'_, 'd, F> {
    fn drain(&self, turn: &mut request::Turn<'_, '_, 'd, (), pool::Cursor>) {
        let (_, mut requests) = turn.parts();
        for port in &self.ports {
            port.drain(&mut requests);
        }
    }
}
