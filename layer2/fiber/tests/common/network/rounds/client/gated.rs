use std::{
    cell::Cell,
    io::{self, Read as _, Write as _},
    net::SocketAddr,
    ops,
    rc::Rc,
};

use dope::{
    core::io::{recv, recv::Lease, transfer, transfer::Length},
    net::{
        tcp::Tcp,
        wire,
        wire::{
            domain,
            domain::Limits,
            failure, open,
            open::Outcome,
            receive,
            receive::{batch, eof, mode, mode::access, reserve},
            send,
            send::{
                Completion, Context, Empty, Payload, Plain, Prepared, Storage, Transition,
                Vectored, reclaim,
            },
            terminal,
            terminal::Terminal,
        },
    },
};
use dope_core::{
    driver::route::{Identity, KeyTag},
    platform::wake,
};
use dope_fiber::net::client::Machine;
use dope_test::harness::{peer, peer::TcpScript, signals};
use o3::{
    buffer,
    buffer::{
        Source, hold,
        storage::range,
        write::{consume, consume::Consume},
    },
};

#[derive(Default)]
pub struct Probe {
    pub(crate) release: Cell<Option<Result<(), wake::NotifyError>>>,
    pub(crate) attempts: Cell<usize>,
    pub(crate) server: Cell<Option<peer::Release>>,
    pub(crate) released: Cell<usize>,
    pub(crate) established: signals::Gate,
    pub(crate) eof: signals::Gate,
    pub(crate) retained: signals::Gate,
    pub(crate) retired: signals::Gate,
}

pub struct Wire {
    pub(crate) established: bool,
    pub(crate) probe: Rc<Probe>,
}

impl Wire {
    pub fn attempt(&mut self) {
        self.probe.attempts.set(self.probe.attempts.get() + 1);
        if let Some(server) = self.probe.server.take() {
            self.probe.release.set(Some(server.try_fire()));
        }
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        self.probe.retired.hit();
    }
}

pub struct Cursor<'d> {
    pub(crate) view: Lease<'d>,
    pub(crate) probe: Rc<Probe>,
}

impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        self.probe.released.set(self.probe.released.get() + 1);
    }
}

impl transfer::Length for Cursor<'_> {
    fn len(&self) -> transfer::Len {
        Length::len(&self.view)
    }
}

impl<'d> buffer::Source for Cursor<'d> {
    type View<'input>
        = <Lease<'d> as buffer::Source>::View<'input>
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        Source::remaining(&self.view)
    }
}

// SAFETY: Immutable fixture views preserve their selected ranges; default retention copies them exactly.
unsafe impl<'d> hold::raw::Retain<'d> for Cursor<'d> {}

impl<'d> buffer::Cursor for Cursor<'d> {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.view.try_get(range)
    }
}

impl AsRef<[u8]> for Cursor<'_> {
    fn as_ref(&self) -> &[u8] {
        self.view.as_ref()
    }
}

impl buffer::Complete for Cursor<'_> {
    fn view(&self) -> Self::View<'_> {
        self.view.view()
    }
}

// SAFETY: all cursor views expose this same driver's exclusive receive range.
unsafe impl buffer::raw::Consume for Cursor<'_> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        // SAFETY: projection preserves the exact admitted owner, range and end.
        Lease::consume(unsafe { prefix.map(|cursor| &mut cursor.view) });
    }
}

impl consume::Consume for Cursor<'_> {
    fn remaining(&self) -> usize {
        Consume::remaining(&self.view)
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.view.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.view.consume_max(max)
    }
}

pub struct Bootstrap;

impl domain::Init<Wire> for Bootstrap {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: Limits,
        config: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        config
    }
}

pub struct Recv;

impl receive::Policy<Wire> for Recv {
    type Batch<'a>
        = batch::One<&'a [u8]>
    where
        Self: 'a;
    type Cursor<'w, 'd>
        = Cursor<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Enabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        let output = if !wire.established {
            wire.established = true;
            wire.probe.established.hit();
            capacity.maybe(None)
        } else {
            capacity.maybe(Some(&*bytes))
        };
        end.output(output)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a,
    {
        let wire = txn.into_wire();
        if !wire.established {
            wire.established = true;
            wire.probe.established.hit();
            return end.output(None);
        }
        let held = Cursor {
            view: bytes,
            probe: Rc::clone(&wire.probe),
        };
        wire.probe.retained.hit();
        end.output(Some(held))
    }
}

impl eof::Eof<Wire> for Recv {
    fn eof<'w, ID: Identity + 'w>(
        wire: &mut Wire,
        end: terminal::Send<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<()>
    where
        Wire: 'w,
    {
        wire.probe.eof.hit();
        end.terminal((), Terminal::Clean)
    }
}

pub struct Send;

impl send::Policy<Wire> for Send {
    type Reclaim = reclaim::OnComplete;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        plain: Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.attempt();
        if !wire.established {
            return end.output(send.empty().into_left::<Plain<'a>>());
        }
        end.output(send.input(plain).into_right::<Empty>())
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.attempt();
        if !wire.established {
            return end.output(send.empty().into_left::<Vectored<'a, N>>());
        }
        end.output(send.vectored(plain).into_right::<Empty>())
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Completion<'a, reclaim::OnComplete, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Transition<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        end.output(Transition::completed(done.into_storage()))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Completion<'a, reclaim::OnComplete, ()>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> Transition<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        Transition::completed(done.into_storage())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.empty())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        _: Option<terminal::Drain<'_, 'd, Wire>>,
    ) -> Prepared<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        send.empty()
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Rc<Probe>;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Rc<Probe>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;

    type Bootstrap = Bootstrap;
    type Receive = Recv;
    type Send = Send;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Rc<Probe> {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Wire, ()) -> open::Disposition<T, Wire, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let wire = Wire {
            established: false,
            probe: Rc::clone(self),
        };
        let output = match consume(context, wire, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        Outcome::Opened(output)
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'p, 'scope, 'd> {
    #[pin]
    #[manifold]
    pub(crate) connector: Machine<'p, 'scope, 'd, 'd, KeyTag<0>, Tcp, Wire>,
}

impl Probe {
    pub fn try_reply(
        reply: &[u8],
        run: impl FnOnce(SocketAddr, Rc<Probe>),
    ) -> Result<io::Result<[u8; 4]>, peer::Finish> {
        TcpScript::try_gated::<1>(
            |[stream]| {
                stream.write_all(b"H")?;
                let mut request = [0; 4];
                stream.read_exact(&mut request)?;
                stream.write_all(reply)?;
                Ok(request)
            },
            |address, release| {
                run(
                    address,
                    Rc::new(Probe {
                        server: Cell::new(Some(release)),
                        ..Probe::default()
                    }),
                );
            },
        )
    }
}
