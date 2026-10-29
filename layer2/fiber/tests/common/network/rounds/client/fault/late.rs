use std::{cell::Cell, rc::Rc};

use dope::{
    core::io::recv,
    net::{
        tcp::Tcp,
        wire,
        wire::{
            domain, failure, open,
            open::Outcome,
            receive,
            receive::{batch, eof, mode, mode::access, reserve, reserve::Completion as _},
            send,
            send::{
                Completion, Context, Payload, Plain, Prepared, Storage, Transition, Vectored,
                reclaim,
            },
            terminal,
            terminal::Terminal,
        },
    },
};
use dope_core::driver::route::{Identity, KeyTag};
use dope_fiber::net::client::Machine;
use dope_test::harness::{signals, signals::Gate};

use crate::common::network::rounds::client::fault::Error;

#[derive(Default)]
pub struct State {
    pub(crate) done: Cell<bool>,
    pub(crate) retired: signals::Gate,
}

impl State {
    pub fn new() -> Self {
        Self {
            done: Cell::new(false),
            retired: Gate::new(),
        }
    }

    pub fn finish(&self) {
        self.done.set(true);
    }

    pub fn is_done(&self) -> bool {
        self.done.get()
    }
}

pub struct Wire;

pub struct Conn(pub(crate) Rc<State>);

impl Drop for Conn {
    fn drop(&mut self) {
        if self.0.is_done() {
            self.0.retired.hit();
        }
    }
}

pub struct Recv;

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
        let send = context.into_state();
        end.output(send.input(plain))
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.vectored(plain))
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

    fn close<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Wire, Storage<'a, ()>>,
        end: terminal::Send<'_, terminal::Error<'d, Wire>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.0.finish();
        end.terminal(
            send.empty(),
            Terminal::Failed(terminal::Error::from_cause(Error::Protocol(
                "late graceful-close failure",
            ))),
        )
    }
}

impl receive::Policy<Wire> for Recv {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Direct;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Guard<ID, Wire>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        txn.complete();
        end.terminal(
            capacity.one(&*bytes),
            Terminal::Failed(terminal::Error::from_cause(Error::Protocol("late failure"))),
        )
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Guard<ID, Wire>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Wire: 'w,
        'd: 'w,
        'w: 'a,
    {
        txn.complete();
        end.terminal(
            Some(bytes.into_shared()),
            Terminal::Failed(terminal::Error::from_cause(Error::Protocol("late failure"))),
        )
    }
}

impl eof::Eof<Wire> for Recv {
    fn eof<'w, ID: Identity + 'w>(
        wire: &mut Conn,
        end: terminal::Send<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<()>
    where
        Wire: 'w,
    {
        wire.0.finish();
        end.terminal(
            (),
            Terminal::Failed(terminal::Error::from_cause(Error::Truncated("late EOF"))),
        )
    }
}

impl failure::Errors<failure::Terminal> for Wire {
    type Error<'d>
        = Error<'d>
    where
        Self: 'd;
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Conn;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Rc<State>;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Rc<State>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Self;
    type Receive = Recv;
    type Send = Send;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Rc<State> {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Conn, ()) -> open::Disposition<T, Conn, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let conn = Conn(Rc::clone(self));
        let output = match consume(context, conn, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        Outcome::Opened(output)
    }
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        bind: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        bind
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
