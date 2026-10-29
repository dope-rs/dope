use std::{marker, ops::ControlFlow};

use dope::{
    core::io::recv,
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
            send, terminal,
            terminal::Terminal,
        },
    },
};
use dope_core::driver::route::{Identity, KeyTag};
use dope_fiber::net::client::Machine;

use crate::common::network::rounds::client::fault::{self, Error};

pub struct Wire<S = bool>(marker::PhantomData<fn() -> S>);

pub trait Observe {
    type Binding;

    fn new(binding: &Self::Binding) -> Self;
    fn observe(&mut self, bytes: usize) -> ControlFlow<()>;
}

impl Observe for bool {
    type Binding = ();

    fn new(_: &()) -> Self {
        false
    }

    fn observe(&mut self, _: usize) -> ControlFlow<()> {
        if *self {
            ControlFlow::Continue(())
        } else {
            *self = true;
            ControlFlow::Break(())
        }
    }
}

pub struct Context<'d, S: Observe = bool> {
    pub(crate) cause: &'d str,
    pub(crate) binding: S::Binding,
}

impl Default for Context<'_, bool> {
    fn default() -> Self {
        let fault::Context(cause) = Default::default();
        Self { cause, binding: () }
    }
}

pub struct Conn<'d, S> {
    state: S,
    cause: &'d str,
}

pub struct Recv;

impl<S: Observe> receive::Policy<Wire<S>> for Recv {
    type Batch<'a>
        = batch::One<&'a [u8]>
    where
        Wire<S>: 'a;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        Wire<S>: 'w,
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire<S>>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire<S>>,
        end: terminal::Both<'_, terminal::Error<'w, Wire<S>>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Wire<S>: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        let output = capacity.one(&*bytes);
        wire.process(bytes.len(), output, end)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire<S>>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire<S>>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Wire<S>: 'w,
        'd: 'w,
        'w: 'a,
    {
        let wire = txn.into_wire();
        let len = bytes.as_slice().len();
        let output = Some(bytes.into_shared());
        wire.process(len, output, end)
    }
}

impl<S: Observe> eof::Eof<Wire<S>> for Recv {
    fn eof<'w, ID: Identity + 'w>(
        wire: &mut Conn<'w, S>,
        end: terminal::Send<'_, terminal::Error<'w, Wire<S>>>,
    ) -> terminal::Processed<()>
    where
        Wire<S>: 'w,
    {
        let cause = terminal::Error::from_cause(Error::Truncated(wire.cause));
        end.terminal((), Terminal::Failed(cause))
    }
}

impl<S: Observe> failure::Errors<failure::Terminal> for Wire<S> {
    type Error<'d>
        = Error<'d>
    where
        Self: 'd;
}

impl<S: Observe> wire::Spec for Wire<S> {
    type Connection<'d, ID: Identity + 'd>
        = Conn<'d, S>
    where
        Self: 'd;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd>
        = Context<'d, S>
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Context<'w, S>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = ()
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Self;
    type Receive = Recv;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, S: Observe + 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire<S>, ID>
    for Context<'w, S>
{
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Conn<'w, S>, ()) -> open::Disposition<T, Conn<'w, S>, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire<S>>> {
        let conn = Conn {
            state: S::new(&self.binding),
            cause: self.cause,
        };
        let output = match consume(context, conn, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        Outcome::Opened(output)
    }
}

impl<S: Observe> domain::Init<Wire<S>> for Wire<S> {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: Limits,
        bind: <Self as wire::Spec>::Binding<'w, ID>,
    ) -> <Self as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire<S>: 'w,
        'd: 'w,
    {
        bind
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'p, 'scope, 'd, S: Observe + 'd> {
    #[pin]
    #[manifold]
    pub(crate) connector: Machine<'p, 'scope, 'd, 'd, KeyTag<0>, Tcp, Wire<S>>,
}

impl<'w, S: Observe + 'w> Conn<'w, S> {
    fn process<T>(
        &mut self,
        bytes: usize,
        output: T,
        end: terminal::Both<'_, terminal::Error<'w, Wire<S>>>,
    ) -> terminal::Processed<T> {
        if self.state.observe(bytes).is_break() {
            let cause = terminal::Error::from_cause(Error::Protocol(self.cause));
            end.terminal(output, Terminal::Failed(cause))
        } else {
            end.output(output)
        }
    }
}
