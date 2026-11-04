use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{route::Identity, schedule::ready::task::owner},
    io::{recv, socket, transfer},
};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{batch, eof, mode, reserve},
    send,
    send::{
        Completion, Context, Payload, Plain, Prepared, Storage, Transition, Vectored,
        progress::Plan, reclaim,
    },
    terminal,
};
use dope_test::harness::{observe::VecCellExt as _, signals::Gate};
use o3::buffer::write::{Sink as _, consume};
use wire::receive::mode::access;

use crate::common::Buffer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Protocol,
    Truncated,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Peer(socket::Error),
    Chunk(Vec<u8>),
    Deferred(Vec<u8>),
    Generation(owner::Lane, owner::Generation),
    Owned(Option<usize>),
    ControlPrepared,
    ControlSent,
    ShutdownRequested,
    PeerEof,
    MissingAdmission,
    Wire(Fault, Option<Fault>, usize),
    Application(dope_manifold::app::receive::Failure),
    Eof,
    Timeout,
    Close,
    Retired,
}

#[derive(Clone, Copy)]
pub enum Mode {
    FailOnEof,
    FailOnData { closing: bool },
    StallOnData,
    CleanOnData,
    CleanOnEof { control: bool },
    EofOnData { control: bool },
}

impl Mode {
    fn control(self) -> Control {
        match self {
            Self::FailOnData { closing: true } => Control::Fail,
            Self::FailOnData { closing: false }
            | Self::CleanOnEof { control: true }
            | Self::EofOnData { control: true } => Control::Send,
            Self::StallOnData => Control::Stall,
            _ => Control::None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Control {
    None,
    Send,
    Stall,
    Fail,
}

pub struct Probe {
    mode: Mode,
    pub(crate) eof: Cell<usize>,
    pub(crate) chunk: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
    pub(crate) writes: Cell<Vec<Result<(), consume::Error>>>,
    pub(crate) admissions: Cell<Vec<Control>>,
}

impl Probe {
    pub fn new(events: Rc<Cell<Vec<Event>>>, mode: Mode) -> Self {
        Self {
            mode,
            eof: Cell::new(0),
            chunk: Gate::new(),
            events,
            writes: Cell::new(Vec::new()),
            admissions: Cell::new(Vec::new()),
        }
    }

    pub fn check(&self) {
        let writes = self.writes.take();
        assert!(
            writes.iter().all(Result::is_ok),
            "wire buffer writes: {:?}",
            writes
        );
        assert!(
            !self.admissions.take().contains(&Control::Fail),
            "a failed closing operation cannot obtain another admission"
        );
    }

    pub(crate) fn observe_eof(&self) {
        self.eof.set(self.eof.get() + 1);
    }

    pub fn eof_count(&self) -> usize {
        self.eof.get()
    }

    pub(crate) fn observe(&self, event: Event) {
        self.events.push(event);
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Rc<Probe> {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Wire, Buffer<64>) -> open::Disposition<T, Wire, Buffer<64>>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let wire = Wire {
            terminal_control: false,
            terminal_inflight: false,
            probe: Rc::clone(self),
        };
        let output = match consume(context, wire, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

pub struct Wire {
    pub(crate) terminal_control: bool,
    pub(crate) terminal_inflight: bool,
    pub(crate) probe: Rc<Probe>,
}

impl Wire {
    pub(crate) fn receive<'d, T>(
        &mut self,
        output: T,
        end: terminal::Both<'_, terminal::Error<'d, Self>>,
    ) -> terminal::Processed<T>
    where
        Self: 'd,
    {
        match self.probe.mode {
            Mode::EofOnData { control } => {
                self.terminal_control = control;
                end.eof(output)
            }
            Mode::CleanOnData => end.terminal(output, terminal::Terminal::Clean),
            Mode::FailOnData { .. } | Mode::StallOnData => {
                self.terminal_control = true;
                end.terminal(
                    output,
                    terminal::Terminal::Failed(terminal::Error::from_cause(Fault::Protocol)),
                )
            }
            Mode::FailOnEof | Mode::CleanOnEof { .. } => end.output(output),
        }
    }
}

pub struct Ingress;

pub struct Egress;

impl send::Policy<Wire> for Egress {
    type Reclaim = reclaim::OnSubmit;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, Buffer<64>>>,
        plain: Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        let mut plan = Plan::plain(send, plain);
        let (store, mut input) = plan.parts();
        let part = input.take(transfer::Len::prefix(
            store.bytes.available(),
            transfer::Len::MAX,
        ));
        wire.probe
            .writes
            .push(store.bytes.write_slice(part.as_slice()));
        part.commit();
        end.output(plan.finish().buffered())
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, Buffer<64>>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        let mut plan = Plan::vectored(send, &plain);
        let (store, mut input) = plan.parts();
        let mut parts = input.take(transfer::Len::prefix(
            store.bytes.available(),
            input.remaining(),
        ));
        for part in parts.take() {
            wire.probe
                .writes
                .push(store.bytes.write_slice(part.bytes()));
        }
        parts.commit();
        end.output(plan.finish().buffered())
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Completion<'a, reclaim::OnSubmit, Buffer<64>>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Transition<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, done) = context.into_parts();
        let send = done.consume_max();
        if wire.terminal_inflight && send.bytes.is_empty() {
            wire.terminal_inflight = false;
            wire.probe.observe(Event::ControlSent);
        }
        end.output(Transition::unchanged(send.consume_none().buffered()))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Completion<'a, reclaim::OnSubmit, Buffer<64>>>,
        end: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> Transition<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let (wire, done) = context.into_parts();
        if end.is_some() {
            wire.probe.admissions.push(wire.probe.mode.control());
        }
        let send = done.consume_max();
        if wire.terminal_inflight && send.bytes.is_empty() {
            wire.terminal_inflight = false;
            wire.probe.observe(Event::ControlSent);
        }
        Transition::unchanged(send.consume_none().buffered())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, Buffer<64>>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, mut send) = context.into_parts();
        if !wire.terminal_control {
            return end.output(send.buffered());
        }
        if wire.probe.mode.control() == Control::Stall {
            return end.output(send.buffered());
        }
        wire.probe.writes.push(send.bytes.write_slice(b"!"));
        wire.terminal_control = false;
        wire.terminal_inflight = true;
        wire.probe.observe(Event::ControlPrepared);
        end.output(send.buffered())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, Buffer<64>>>,
        end: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> Prepared<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let (wire, mut send) = context.into_parts();
        if !wire.terminal_control || wire.probe.mode.control() == Control::Stall {
            return send.buffered();
        }
        wire.probe.writes.push(send.bytes.write_slice(b"!"));
        wire.terminal_control = false;
        wire.terminal_inflight = true;
        wire.probe.observe(Event::ControlPrepared);
        if wire.probe.mode.control() == Control::Fail {
            match end {
                Some(end) => end.fail(Fault::Closing),
                None => wire.probe.observe(Event::MissingAdmission),
            }
        }
        send.buffered()
    }
}

impl receive::Policy<Wire> for Ingress {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        wire.receive(capacity.one(&*bytes), end)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        'd: 'w,
        'w: 'a,
    {
        let wire = txn.into_wire();
        wire.receive(Some(bytes.into_shared()), end)
    }
}

impl eof::Eof<Wire> for Ingress {
    fn eof<'w, ID: Identity + 'w>(
        wire: &mut Wire,
        end: terminal::Send<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<()> {
        wire.probe.observe_eof();
        wire.terminal_control = wire.probe.mode.control() != Control::None;
        match wire.probe.mode {
            Mode::FailOnEof => end.terminal(
                (),
                terminal::Terminal::Failed(terminal::Error::from_cause(Fault::Truncated)),
            ),
            _ => end.terminal((), terminal::Terminal::Clean),
        }
    }
}

impl failure::Errors<failure::Terminal> for Wire {
    type Error<'d>
        = Fault
    where
        Self: 'd;
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd>
        = Self
    where
        Self: 'd;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd>
        = Rc<Probe>
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Rc<Probe>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = Buffer<64>
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Self;
    type Receive = Ingress;
    type Send = Egress;
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        bind: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        'd: 'w,
    {
        bind
    }
}
