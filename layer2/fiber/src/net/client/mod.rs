use std::mem;

use dope::{
    core::driver::route::{Identity, KeyTag},
    net::{
        self,
        link::{
            event,
            pool::{
                change::{self, opening},
                recv::input,
            },
        },
        wire::{self, domain, open, receive::Chunk, terminal},
    },
};

use crate::{
    context::wake,
    core::{driver, io::socket},
    manifold::{
        app::{creation, env},
        client::{
            attempt::{
                self, queue,
                queue::mailbox::{self, Error},
            },
            conn::{self, engine},
        },
        timing,
    },
    net::port::{active::Activation, table::Table},
};

mod app;
mod connect;
mod plan;

pub use app::App;
pub use connect::Connect;
pub use plan::Plan;

/// Stateless per-connection value used by the port-backed client machine.
pub struct State;

/// Failure of a connection attempt returned to its caller.
#[derive(Debug)]
pub enum ConnectError<'d, E, ID: Identity = KeyTag<0>> {
    Activation,
    Mailbox(Error<'d, ID>),
    Transport(event::ConnectFault),
    Open(opening::Failure<E>),
}

impl<'d, E, ID: Identity> ConnectError<'d, E, ID> {
    /// Borrows the wire-open cause while retaining exact mailbox and transport failures.
    pub fn map_ref<'a, F>(&'a self, map: impl FnOnce(&'a E) -> F) -> ConnectError<'d, F, ID> {
        match self {
            Self::Activation => ConnectError::Activation,
            Self::Mailbox(cause) => ConnectError::Mailbox(*cause),
            Self::Transport(cause) => ConnectError::Transport(*cause),
            Self::Open(cause) => ConnectError::Open(match cause {
                opening::Failure::IdentityExhausted => opening::Failure::IdentityExhausted,
                opening::Failure::Submission(cause) => opening::Failure::Submission(*cause),
                opening::Failure::Wire(cause) => opening::Failure::Wire(map(cause)),
            }),
        }
    }
}

enum Outcome<'d, ID: Identity, E> {
    Connected(conn::Id<'d, ID>),
    Activation,
    Transport(event::ConnectFault),
    Open(opening::Failure<E>),
}

type Ticket<'a, 'w, 'd, T, W, ID> =
    mailbox::Lease<'a, 'd, T, (), Outcome<'d, ID, open::Error<'w, W>>, ID>;

type Connected<'a, 'w, 'd, W, ID> = Result<
    crate::net::Io<'a, 'w, 'd, W, conn::Id<'d, ID>>,
    ConnectError<'d, open::Error<'w, W>, ID>,
>;

pub struct Port<'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity = KeyTag<0>> {
    conns: Table<'d, Chunk<'w, 'd, W>, terminal::Error<'w, W>, conn::Id<'d, ID>>,
    pending: mailbox::Domain<'d, T, (), Outcome<'d, ID, open::Error<'w, W>>, ID>,
}

impl<'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity> Port<'w, 'd, T, W, ID> {
    fn from_parts(
        conns: Table<'d, Chunk<'w, 'd, W>, terminal::Error<'w, W>, conn::Id<'d, ID>>,
        pending: mailbox::Domain<'d, T, (), Outcome<'d, ID, open::Error<'w, W>>, ID>,
    ) -> Self {
        Self { conns, pending }
    }

    pub fn handle(&self) -> Handle<'_, 'w, 'd, T, W, ID> {
        Handle { port: self }
    }

    pub fn machine<'scope, C, O>(
        &'scope self,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'p> FnOnce(Machine<'p, 'scope, 'w, 'd, ID, T, W>, &mut C) -> O,
    ) -> Result<O, creation::Error>
    where
        ID: 'd,
        W: input::Execution + 'd,
        W::Bootstrap: domain::Init<W>,
        W::Binding<'w, ID>: Default,
    {
        self.machine_in(
            domain::Local::new(Default::default()),
            owner,
            context,
            build,
        )
    }

    pub fn machine_in<'scope, R, C, O>(
        &'scope self,
        endpoint: R,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'p> FnOnce(Machine<'p, 'scope, 'w, 'd, ID, T, W>, &mut C) -> O,
    ) -> Result<O, creation::Error>
    where
        ID: 'd,
        W: input::Execution + 'd,
        R: change::Prepare<'w, 'd, ID, W>,
    {
        let conns = self.conns.capacity();
        engine::Startup::new(
            App { port: self },
            self.pending.source(),
            conns,
            Default::default(),
            endpoint,
        )
        .try_with(owner, context, build)
    }

    fn dial<'source>(
        &'source self,
        addr: T::Addr,
    ) -> Result<Ticket<'source, 'w, 'd, T, W, ID>, ConnectError<'d, open::Error<'w, W>, ID>> {
        use socket::option::Set;

        self.pending
            .try_reserve()
            .map(|vacant| vacant.start((), attempt::StreamTarget::new(addr, Set::EMPTY)))
            .map_err(ConnectError::Mailbox)
    }

    fn reclaim(&self, outcome: Outcome<'d, ID, open::Error<'w, W>>) {
        if let Outcome::Connected(conn) = outcome {
            self.conns.channel().close(conn);
        }
    }

    fn complete(
        &self,
        key: attempt::Completion<'_, 'd, ID>,
        outcome: Outcome<'d, ID, open::Error<'w, W>>,
    ) {
        match self.pending.try_settle(&key, outcome) {
            Ok(((), Some(waiter))) => waiter.wake(),
            Ok(((), None)) => {}
            Err((_, outcome)) => self.reclaim(outcome),
        }
    }

    fn connected(
        &self,
        key: attempt::Completion<'_, 'd, ID>,
        conn: conn::Id<'d, ID>,
        wake: wake::RootWaker<'d>,
    ) {
        if self.conns.activate(conn, wake) == Activation::Rejected {
            self.complete(key, Outcome::Activation);
            return;
        }
        self.complete(key, Outcome::Connected(conn));
    }
}

pub type Machine<'p, 'scope, 'w, 'd, ID, T, W> = engine::Machine<
    'p,
    'w,
    'd,
    ID,
    App<'scope, 'w, 'd, T, W, ID>,
    queue::Control<'scope, 'd, T, ID>,
    env::Bundle<T, W, timing::Balanced>,
>;

pub struct Handle<'scope, 'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity = KeyTag<0>> {
    port: &'scope Port<'w, 'd, T, W, ID>,
}

impl<T: net::Transport, W: wire::Spec, ID: Identity> Copy for Handle<'_, '_, '_, T, W, ID> {}

impl<T: net::Transport, W: wire::Spec, ID: Identity> Clone for Handle<'_, '_, '_, T, W, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'scope, 'w, 'd: 'w, T: net::Transport, W: wire::Spec, ID: Identity>
    Handle<'scope, 'w, 'd, T, W, ID>
{
    pub fn connect(self, addr: T::Addr) -> Connect<'scope, 'w, 'd, T, W, ID> {
        Connect::new(self, addr)
    }
}

const _: () = assert!(
    mem::size_of::<ConnectError<'static, open::Error<'static, wire::Identity>>>()
        <= 4 * mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Outcome<'static, KeyTag<0>, open::Error<'static, wire::Identity>>>()
        == 3 * mem::size_of::<usize>()
);
const _: () = assert!(
    mem::size_of::<Connect<'static, 'static, 'static, net::tcp::Tcp, wire::Identity>>()
        == 5 * mem::size_of::<usize>()
);
