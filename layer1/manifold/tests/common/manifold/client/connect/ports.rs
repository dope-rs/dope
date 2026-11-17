use std::{cell::Cell, marker::PhantomData, rc::Rc};

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
    link::{egress, pool::{change::opening, recv::input}},
    wire::{failure, open, terminal},
};
use dope_test::harness::signals::Gate;
use o3::{
    base::{Call, cell::region},
    buffer::{bytes::TryRetain, pool, storage},
    mem::rc,
};

use crate::common::{
    contract::Report,
    manifold::client::{connect::Error, error, send::wire::Wire},
};

pub struct App<'e, 'd, F, W = Wire> {
    pub(crate) ports: Vec<Port<'e, 'd, Payload>>,
    pub(crate) call: Option<F>,
    pub(crate) done: Gate,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) closed: Gate,
    pub(crate) reason: Rc<Cell<Option<close::Reason>>>,
    wire: PhantomData<fn() -> W>,
}

pub const PAYLOAD: &[u8] = b"payload";
pub const NESTED: &[u8] = b"nested";

pub type Payload = egress::data::pair::Source<storage::Shared>;

pub fn pair(buffers: &pool::Pool, body: storage::Shared) -> Payload {
    let mut prefix = buffers.cursor().expect("header slot");
    prefix.try_append(b"h").expect("header bytes").commit();
    egress::data::pair::Source::try_new(prefix, body)
        .map_err(|rejected| rejected.into_parts().2)
        .expect("header and body")
        .into_inner()
}

impl<'e, 'd, F, W> App<'e, 'd, F, W>
where
    F: for<'turn, 'queue> FnOnce(
        &[Port<'e, 'd, Payload>],
        conn::Id<'d, KeyTag<0>>,
        Work<'turn, 'd>,
        egress::Queue<'_, 'queue, 'd, 32, Payload>,
        &mut region::Token<region::Identity<'d>>,
    ) -> Option<close::Directive>,
{
    pub fn new(
        ports: Vec<Port<'e, 'd, Payload>>,
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
            wire: PhantomData,
        }
    }
}

impl<'d, F, W: input::Execution + 'd> app::Application<'d> for App<'_, 'd, F, W> {
    type Conn = ();
    type Connect = Full;
    type Deferred = Controlled;
    type Failure = Observed;
    type Wire = W;
    type Send = Payload;
    type Input = receive::Borrowed;
    type Requests = request::Enabled;

    fn try_conn(&self) -> Result<(), rc::Error> {
        Ok(())
    }
}

impl<'d, F, W: input::Execution + 'd> app::Receive<'d> for App<'_, 'd, F, W> {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, KeyTag<0>>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, KeyTag<0>> {
        barrier.ready()
    }
}

impl<'d, F, W: input::Execution + 'd> app::Borrowed<'d> for App<'_, 'd, F, W> {
    fn chunk<O, R: TryRetain>(
        &mut self,
        conn: conn::Ctx<'_, 'd, KeyTag<0>, W, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, Payload>,
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

impl<'d, F, W: input::Execution + 'd> admit::Register<'d> for App<'_, 'd, F, W> {
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

impl<'d, F, W: input::Execution + 'd> app::Connected<'d> for App<'_, 'd, F, W> {
    type Activation = admit::Registered;

    fn connected<O>(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        conn: conn::Established<'_, 'd, KeyTag<0>, W, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, Payload>,
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

impl<'d, F, W: input::Execution + 'd> app::Exhausted<'d> for App<'_, 'd, F, W> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, F, W: input::Execution + 'd> app::Failed<'d> for App<'_, 'd, F, W> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(cause));
    }
}

impl<'d, F, W> app::Opened<'d, 'd> for App<'_, 'd, F, W>
where
    W: input::Execution<OpenFailure = failure::Never> + 'd,
{
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: open::Error<'d, W>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'e, 'd, F, W> app::Lifecycle<'d, 'd> for App<'e, 'd, F, W>
where
    W: input::Execution<TerminalFailure = failure::Never> + 'd,
    F: for<'turn, 'queue> FnOnce(
        &[Port<'e, 'd, Payload>],
        conn::Id<'d, KeyTag<0>>,
        Work<'turn, 'd>,
        egress::Queue<'_, 'queue, 'd, 32, Payload>,
        &mut region::Token<region::Identity<'d>>,
    ) -> Option<close::Directive>,
{
    fn take_close(&self, conn: conn::Id<'d, KeyTag<0>>) -> Option<close::Directive> {
        self.ports.iter().find_map(|port| port.take_close(conn))
    }

    fn prepare<O>(
        &mut self,
        mut context: conn::Ctx<'_, 'd, KeyTag<0>, W, (), O>,
        _: bool,
        queue: egress::Queue<'_, '_, 'd, 32, Payload>,
        driver: &mut driver::Context<'_, 'd>,
        _: &<Self::Wire as dope_net::wire::Spec>::Connection<
            'd,
            dope_core::driver::route::KeyTag<0>,
        >,
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

    fn cancel(&mut self, _: Payload, _: &mut region::Token<region::Identity<'d, 1>>) {
        self.audit.cancelled();
    }

    fn wire_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, W, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, Payload>,
        cause: terminal::Error<'d, W>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        cause.eliminate()
    }

    fn io_fail<O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, W, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, Payload>,
        cause: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(cause));
    }

    fn close<O>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, W, (), O>,
        _: egress::Queue<'_, '_, 'd, 32, Payload>,
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

impl<'d, F, W: input::Execution + 'd> app::Scheduling<'d> for App<'_, 'd, F, W> {
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

impl<'d, F, W: input::Execution + 'd> app::Requests<'d> for App<'_, 'd, F, W> {
    fn drain(&self, turn: &mut request::Turn<'_, '_, 'd, (), Payload>) {
        let (_, mut requests) = turn.parts();
        for port in &self.ports {
            port.drain(&mut requests);
        }
    }
}
