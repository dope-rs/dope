use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{
        self,
        receive::credit,
        route::{Identity, table::conn::Capacity},
        schedule::{self, clock},
    },
    io::{recv, socket},
};
use dope_manifold::{
    app::receive,
    client::{
        app::{
            self, Application, ChunkOutcome, admit::Automatic, connect::Full, defer::Controlled,
            failure::Observed, request, resume,
        },
        attempt, close, conn,
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{
        self, domain,
        domain::Limits,
        failure, open,
        receive::{Policy, batch, eof, mode, mode::access, reserve},
        send, terminal,
    },
};
use dope_test::harness::signals::{Gate, Progress as _};
use o3::{
    base::cell::region,
    buffer::{bytes::TryRetain, storage::Shared},
    depot::queue::tagged::entry,
    mem::rc,
};

use crate::common::{
    contract::Report,
    manifold::client::{connect::Error, error},
};

pub struct Wire;
pub struct Wait;
pub struct Receive;
pub enum Reservation {}
pub struct Runtime {
    pub(crate) limits: Limits,
    pub(crate) recv: usize,
    pub(crate) send: usize,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) dropped: Rc<Cell<usize>>,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if self.recv != 0 || self.send != 0 {
            self.audit.faults.record(
                Error::CreditRelease {
                    received: self.recv,
                    sent: self.send,
                }
                .into(),
            );
        }
        self.dropped.set(self.dropped.get() + 1);
    }
}

impl reserve::Registration for Reservation {
    fn commit(self) {
        match self {}
    }
}

impl reserve::Completion for Reservation {
    fn complete(self) {
        match self {}
    }
}

impl<'d, ID: Identity> reserve::Wait<'d, ID, Wire> for Wait {
    type Registration = Reservation;

    fn register(
        self,
        _: credit::Pending<'d, ID>,
    ) -> Result<Self::Registration, entry::Error<Capacity>> {
        Err(entry::Error::Index {
            index: 1,
            capacity: Capacity::fixed::<1>(),
        })
    }
}

impl reserve::Try<Wire> for Receive {
    type Block<'a, 'w, 'd, ID: Identity + 'w>
        = Wait
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
        = Reservation
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a;

    fn try_reserve<'a, 'w, 'd, ID: Identity + 'w>(
        _: &'a mut wire::Identity,
        _: &'a mut (),
        _: &'a mut Runtime,
    ) -> reserve::Outcome<
        Self::Transaction<'a, 'w, 'd, ID>,
        Self::Block<'a, 'w, 'd, ID>,
        terminal::Error<'w, Wire>,
    >
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a,
    {
        reserve::Outcome::Blocked(Wait)
    }

    fn cancel<'w, 'd, ID: Identity + 'w>(run: &mut Runtime, target: credit::Id<'d, ID>)
    where
        Wire: 'w,
        'd: 'w,
    {
        let slot = target.slot().index().get();
        let capacity = run.limits.connections().count().get();
        if slot >= capacity {
            run.audit
                .faults
                .record(Error::CreditTarget { slot, capacity }.into());
        }
    }

    fn release_recv<'w, 'd, ID: Identity + 'w>(run: &mut Runtime)
    where
        Wire: 'w,
        'd: 'w,
    {
        run.recv += 1;
    }

    fn release_send<'w, 'd, ID: Identity + 'w>(run: &mut Runtime)
    where
        Wire: 'w,
        'd: 'w,
    {
        run.send += 1;
    }
}

impl Policy<Wire> for Receive {
    type Batch<'input> = batch::One<&'input [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Waiting<Self>;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: Reservation,
        _: &'input mut [u8],
        _: batch::Grant<'id, 'd, Wire>,
        _: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        'd: 'w,
        'w: 'a + 'input,
    {
        match txn {}
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: Reservation,
        _: recv::Lease<'d>,
        _: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        'd: 'w,
        'w: 'a,
    {
        match txn {}
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Runtime {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, wire::Identity, ()) -> open::Disposition<T, wire::Identity, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let output = match consume(context, wire::Identity, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = wire::Identity;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = (Rc<Report<error::Error, 16>>, Rc<Cell<usize>>);
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Runtime
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;

    type Bootstrap = Self;
    type Receive = Receive;
    type Send = send::Direct;
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        limits: Limits,
        (audit, dropped): <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        Runtime {
            limits,
            recv: 0,
            send: 0,
            audit,
            dropped,
        }
    }
}

pub struct State;

pub struct Parked {
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) closed: Gate,
    pub(crate) live: Cell<bool>,
}

impl<'d, ID: Identity> Application<'d, ID> for Parked {
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

impl<'d, ID: Identity> app::Receive<'d, ID> for Parked {
    type Continuation = resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, ID>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, ID> {
        barrier.ready()
    }
}

impl<'d, ID: Identity> app::Borrowed<'d, ID> for Parked {
    fn chunk<'e, O, R: TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, 'e, 'd, 32, Shared>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> ChunkOutcome {
        self.audit.faults.record(Error::Received.into());
        ChunkOutcome::ok()
    }
}

impl<'d, ID: Identity> app::Connected<'d, ID> for Parked {
    type Activation = Automatic;

    fn connected<'e, O>(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        _: conn::Established<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, 'e, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let live = self.live.replace(true);
        if live {
            self.audit.faults.record(Error::Connected(live).into());
        }
    }
}

impl<'d, ID: Identity> app::Exhausted<'d, ID> for Parked {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Open(cause));
    }
}

impl<'d, ID: Identity> app::Failed<'d, ID> for Parked {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d, ID: Identity> app::Opened<'d, 'd, ID> for Parked {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        error: open::Error<'d, Wire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }
}

impl<'d, ID: Identity> app::Lifecycle<'d, 'd, ID> for Parked {
    fn prepare<'e, O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, 'e, 'd, 32, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let live = self.live.get();
        if !live {
            self.audit.faults.record(Error::Preparation(live).into());
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

    fn wire_fail<'e, O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, 'e, 'd, 32, Shared>,
        error: terminal::Error<'d, Wire>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        error.eliminate()
    }

    fn io_fail<'e, O>(
        &mut self,
        _: conn::Io<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, 'e, 'd, 32, Shared>,
        error: dope_core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn close<'e, O>(
        &mut self,
        _: conn::Ctx<'_, 'd, ID, Wire, State, O>,
        _: egress::Queue<'_, 'e, 'd, 32, Shared>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        let live = self.live.replace(false);
        if !live {
            self.audit.faults.record(Error::Closing(live).into());
        }
        self.closed.hit();
        app::CloseOutcome::Complete(close::Directive::reconnect(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, ID, Self>,
        _: &mut State,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let live = self.live.get();
        let closed = self.closed.hits();
        if live || closed != 1 {
            self.audit
                .faults
                .record(Error::Retired { live, closed }.into());
        }
    }
}

impl<'d, ID: Identity> app::Scheduling<'d, ID> for Parked {
    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> schedule::Progress<'d> {
        schedule::Progress::Quiescent
    }
}
