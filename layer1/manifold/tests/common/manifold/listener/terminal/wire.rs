use std::{cell::Cell, marker::PhantomData, rc::Rc};

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

#[derive(Clone, PartialEq, Eq)]
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
    Eof,
    Timeout,
    Close,
    Retired,
}

pub trait FailureMode {
    const DATA: Option<Fault>;
    const EOF: Option<Fault>;
    const CONTROL: Control;
    const CLEAN_DATA: bool = false;
    const EOF_DATA: bool = false;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Control {
    None,
    Send,
    Stall,
    Fail,
}

pub struct Probe {
    pub(crate) eof: Cell<usize>,
    pub(crate) chunk: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
    pub(crate) writes: Cell<Vec<Result<(), consume::Error>>>,
    pub(crate) admissions: Cell<Vec<Control>>,
}

impl Probe {
    pub fn new(events: Rc<Cell<Vec<Event>>>) -> Self {
        Self {
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

impl<'w, 'd: 'w, M: FailureMode + 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire<M>, ID>
    for Rc<Probe>
{
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Wire<M>, Buffer<64>) -> open::Disposition<T, Wire<M>, Buffer<64>>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire<M>>> {
        let wire = Wire {
            terminal_control: false,
            terminal_inflight: false,
            probe: Rc::clone(self),
            mode: PhantomData,
        };
        let output = match consume(context, wire, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

pub struct FailOnEof;

impl FailureMode for FailOnEof {
    const DATA: Option<Fault> = None;
    const EOF: Option<Fault> = Some(Fault::Truncated);
    const CONTROL: Control = Control::None;
}

pub struct FailOnData<const FAIL: bool = false>;

impl<const FAIL: bool> FailureMode for FailOnData<FAIL> {
    const DATA: Option<Fault> = Some(Fault::Protocol);
    const EOF: Option<Fault> = None;
    const CONTROL: Control = if FAIL { Control::Fail } else { Control::Send };
}

pub struct StallOnData;

impl FailureMode for StallOnData {
    const DATA: Option<Fault> = Some(Fault::Protocol);
    const EOF: Option<Fault> = None;
    const CONTROL: Control = Control::Stall;
}

pub struct CleanOnData;

impl FailureMode for CleanOnData {
    const DATA: Option<Fault> = None;
    const EOF: Option<Fault> = None;
    const CONTROL: Control = Control::None;
    const CLEAN_DATA: bool = true;
}

pub struct CleanOnEof<const SEND: bool = false>;

impl<const SEND: bool> FailureMode for CleanOnEof<SEND> {
    const DATA: Option<Fault> = None;
    const EOF: Option<Fault> = None;
    const CONTROL: Control = if SEND { Control::Send } else { Control::None };
}

pub struct EofOnData<const SEND: bool = false>;

impl<const SEND: bool> FailureMode for EofOnData<SEND> {
    const DATA: Option<Fault> = None;
    const EOF: Option<Fault> = None;
    const CONTROL: Control = if SEND { Control::Send } else { Control::None };
    const EOF_DATA: bool = true;
}

pub struct Wire<M> {
    pub(crate) terminal_control: bool,
    pub(crate) terminal_inflight: bool,
    pub(crate) probe: Rc<Probe>,
    pub(crate) mode: PhantomData<M>,
}

impl<M: FailureMode> Wire<M> {
    pub(crate) fn receive<'d, T>(
        &mut self,
        output: T,
        end: terminal::Both<'_, terminal::Error<'d, Self>>,
    ) -> terminal::Processed<T>
    where
        Self: 'd,
    {
        if M::EOF_DATA {
            self.terminal_control = M::CONTROL != Control::None;
            return end.eof(output);
        }
        if M::CLEAN_DATA {
            return end.terminal(output, terminal::Terminal::Clean);
        }
        match M::DATA {
            Some(cause) => {
                self.terminal_control = M::CONTROL != Control::None;
                end.terminal(
                    output,
                    terminal::Terminal::Failed(terminal::Error::from_cause(cause)),
                )
            }
            None => end.output(output),
        }
    }
}

pub struct Ingress<M>(pub(crate) PhantomData<M>);

pub struct Egress;

impl<M: FailureMode> send::Policy<Wire<M>> for Egress {
    type Reclaim = reclaim::OnSubmit;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire<M>, Storage<'a, Buffer<64>>>,
        plain: Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire<M>>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Wire<M>: 'd,
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
        context: Context<'a, 'd, ID, Wire<M>, Storage<'a, Buffer<64>>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire<M>>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Wire<M>: 'd,
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
        context: Context<'a, 'd, ID, Wire<M>, Completion<'a, reclaim::OnSubmit, Buffer<64>>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire<M>>>,
    ) -> terminal::Processed<Transition<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Wire<M>: 'd,
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
        context: Context<'a, 'd, ID, Wire<M>, Completion<'a, reclaim::OnSubmit, Buffer<64>>>,
        end: Option<terminal::Drain<'_, 'd, Wire<M>>>,
    ) -> Transition<'a, Self::Reclaim, impl Payload<'a>>
    where
        Wire<M>: 'd,
        'd: 'a,
    {
        let (wire, done) = context.into_parts();
        if end.is_some() {
            wire.probe.admissions.push(M::CONTROL);
        }
        let send = done.consume_max();
        if wire.terminal_inflight && send.bytes.is_empty() {
            wire.terminal_inflight = false;
            wire.probe.observe(Event::ControlSent);
        }
        Transition::unchanged(send.consume_none().buffered())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire<M>, Storage<'a, Buffer<64>>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire<M>>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Wire<M>: 'd,
        'd: 'a,
    {
        let (wire, mut send) = context.into_parts();
        if !wire.terminal_control {
            return end.output(send.buffered());
        }
        if M::CONTROL == Control::Stall {
            return end.output(send.buffered());
        }
        wire.probe.writes.push(send.bytes.write_slice(b"!"));
        wire.terminal_control = false;
        wire.terminal_inflight = true;
        wire.probe.observe(Event::ControlPrepared);
        end.output(send.buffered())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire<M>, Storage<'a, Buffer<64>>>,
        end: Option<terminal::Drain<'_, 'd, Wire<M>>>,
    ) -> Prepared<'a, Self::Reclaim, impl Payload<'a>>
    where
        Wire<M>: 'd,
        'd: 'a,
    {
        let (wire, mut send) = context.into_parts();
        if !wire.terminal_control || M::CONTROL == Control::Stall {
            return send.buffered();
        }
        wire.probe.writes.push(send.bytes.write_slice(b"!"));
        wire.terminal_control = false;
        wire.terminal_inflight = true;
        wire.probe.observe(Event::ControlPrepared);
        if M::CONTROL == Control::Fail {
            match end {
                Some(end) => end.fail(Fault::Closing),
                None => wire.probe.observe(Event::MissingAdmission),
            }
        }
        send.buffered()
    }
}

impl<M: FailureMode> receive::Policy<Wire<M>> for Ingress<M> {
    type Batch<'a>
        = batch::One<&'a [u8]>
    where
        Wire<M>: 'a;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        Wire<M>: 'w,
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire<M>>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire<M>>,
        end: terminal::Both<'_, terminal::Error<'w, Wire<M>>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Wire<M>: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        wire.receive(capacity.one(&*bytes), end)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire<M>>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire<M>>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Wire<M>: 'w,
        'd: 'w,
        'w: 'a,
    {
        let wire = txn.into_wire();
        wire.receive(Some(bytes.into_shared()), end)
    }
}

impl<M: FailureMode> eof::Eof<Wire<M>> for Ingress<M> {
    fn eof<'w, ID: Identity + 'w>(
        wire: &mut Wire<M>,
        end: terminal::Send<'_, terminal::Error<'w, Wire<M>>>,
    ) -> terminal::Processed<()>
    where
        Wire<M>: 'w,
    {
        wire.probe.observe_eof();
        wire.terminal_control = M::CONTROL != Control::None;
        match M::EOF {
            Some(cause) => end.terminal(
                (),
                terminal::Terminal::Failed(terminal::Error::from_cause(cause)),
            ),
            None => end.terminal((), terminal::Terminal::Clean),
        }
    }
}

impl<M: FailureMode> failure::Errors<failure::Terminal> for Wire<M> {
    type Error<'d>
        = Fault
    where
        Self: 'd;
}

impl<M: FailureMode> wire::Spec for Wire<M> {
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
    type Receive = Ingress<M>;
    type Send = Egress;
}

impl<M: FailureMode> domain::Init<Wire<M>> for Wire<M> {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        bind: <Wire<M> as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire<M> as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire<M>: 'w,
        'd: 'w,
    {
        bind
    }
}
