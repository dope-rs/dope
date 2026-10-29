use std::{cell::Cell, convert::Infallible, ops, rc::Rc};

use dope::{
    core::io::{recv::Lease, transfer, transfer::Len},
    net,
    net::{
        tcp::Tcp,
        wire,
        wire::{
            domain, failure, open,
            open::Outcome,
            receive,
            receive::{batch, eof, mode, mode::access, reserve},
            send, terminal,
            terminal::Terminal,
        },
    },
};
use dope_core::driver::route::{Identity, KeyTag};
use dope_fiber::net::server::Listener;
use dope_test::harness::signals::Gate;
use o3::{
    buffer,
    buffer::{
        hold, pool,
        pool::state,
        storage,
        storage::{Shared, range},
        write::{consume, consume::Consume},
    },
};

pub const MAX_CONN: usize = 2;
pub const QUEUE_CAP: usize = 256;

#[derive(Default)]
pub struct PooledWire;

pub struct Bootstrap;

pub struct Endpoint {
    pub(crate) pool: pool::Pool,
    pub(crate) seen: Gate,
    pub(crate) limits: Rc<Cell<Option<domain::Limits>>>,
}

impl Endpoint {
    pub fn try_new(seen: Gate) -> Result<Self, pool::CreateError> {
        Ok(Self {
            pool: pool::Pool::<state::Uninit>::try_new(QUEUE_CAP + 1, 1)?,
            seen,
            limits: Rc::new(Cell::new(None)),
        })
    }

    pub fn try_retain(&self, bytes: &[u8]) -> Result<Option<storage::Shared>, consume::Error> {
        let Some(value) = self.pool.try_copy(bytes)? else {
            return Ok(None);
        };
        let value = value.into_shared();
        if bytes == b"H" {
            self.seen.hit();
        }
        Ok(Some(value))
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, PooledWire, ID> for &'w Endpoint {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, &'w Endpoint, ()) -> open::Disposition<T, &'w Endpoint, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, PooledWire>> {
        let output = match consume(context, self, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        Outcome::Opened(output)
    }
}

impl domain::Bootstrap<PooledWire> for Bootstrap {
    fn try_runtime<'w, 'd, ID: Identity + 'w>(
        limits: domain::Limits,
        owner: <PooledWire as wire::Spec>::Binding<'w, ID>,
    ) -> Result<<PooledWire as wire::Spec>::Runtime<'w, 'd, ID>, net::Resource>
    where
        PooledWire: 'w,
        'd: 'w,
    {
        owner.limits.set(Some(limits));
        Ok(owner)
    }
}

#[repr(transparent)]
pub struct Cursor(pub(crate) storage::Shared);

impl transfer::Length for Cursor {
    fn len(&self) -> transfer::Len {
        Len::prefix(self.0.as_slice().len(), Len::MAX)
    }
}

impl buffer::Source for Cursor {
    type View<'input>
        = &'input [u8]
    where
        Self: 'input;

    fn remaining(&self) -> usize {
        self.0.as_slice().len()
    }
}

// SAFETY: Immutable fixture views preserve their selected ranges; default retention copies them exactly.
unsafe impl<'d> hold::raw::Retain<'d> for Cursor {}

impl buffer::Cursor for Cursor {
    fn try_get<'input>(
        &'input self,
        range: ops::Range<usize>,
    ) -> Result<Self::View<'input>, range::Error> {
        self.0.as_slice().try_get(range)
    }
}

impl AsRef<[u8]> for Cursor {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl buffer::Complete for Cursor {
    fn view(&self) -> Self::View<'_> {
        self.0.as_slice()
    }
}

// SAFETY: every source view exposes the inner owner's exact current range.
unsafe impl buffer::raw::Consume for Cursor {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        // SAFETY: this transparent projection preserves the admitted owner and end.
        <Shared as buffer::raw::Consume>::consume(unsafe { prefix.map(|cursor| &mut cursor.0) });
    }
}

impl Consume for Cursor {
    fn remaining(&self) -> usize {
        Consume::remaining(&self.0)
    }

    fn try_consume(&mut self, amount: usize) -> Result<(), consume::Error> {
        self.0.try_consume(amount)
    }

    fn consume_max(&mut self, max: usize) -> usize {
        self.0.consume_max(max)
    }
}

pub struct Ingress;

impl Ingress {
    fn finish<'w>(
        chunk: Result<Option<storage::Shared>, consume::Error>,
        end: terminal::Both<'_, terminal::Error<'w, PooledWire>>,
    ) -> terminal::Processed<Option<storage::Shared>> {
        match chunk {
            Ok(chunk) => end.output(chunk),
            Err(cause) => end.terminal(None, Terminal::Failed(terminal::Error::from_cause(cause))),
        }
    }
}

impl receive::Policy<PooledWire> for Ingress {
    type Batch<'a>
        = batch::One<storage::Shared>
    where
        Self: 'a;
    type Cursor<'w, 'd>
        = Cursor
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, PooledWire>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, PooledWire>,
        end: terminal::Both<'_, terminal::Error<'w, PooledWire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        PooledWire: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let pool = txn.into_wire();
        Self::finish(pool.try_retain(bytes), end).map(|chunk| capacity.maybe(chunk))
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, PooledWire>,
        bytes: Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, PooledWire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        PooledWire: 'w,
        'd: 'w,
        'w: 'a,
    {
        let pool = txn.into_wire();
        let chunk = {
            let bytes = bytes;
            pool.try_retain(bytes.as_slice())
        };
        Self::finish(chunk, end).map(|chunk| chunk.map(Cursor))
    }
}

impl failure::Errors<failure::Terminal> for PooledWire {
    type Error<'d> = consume::Error;
}

impl wire::Spec for PooledWire {
    type Connection<'d, ID: Identity + 'd> = &'d Endpoint;
    type Storage<ID: Identity> = Endpoint;
    type Binding<'d, ID: Identity + 'd> = &'d Endpoint;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = &'w Endpoint
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;

    type Bootstrap = Bootstrap;
    type Receive = Ingress;
    type Send = send::Direct;
}

impl<ID: Identity> domain::Plan<PooledWire, ID> for () {
    type Error = Infallible;

    fn bind<'d>(self, owner: &'d Endpoint) -> Result<&'d Endpoint, Self::Error>
    where
        PooledWire: 'd,
        ID: 'd,
    {
        Ok(owner)
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'n, 'p, 's, 'scope, 'w, 'd: 's + 'w, F: FnMut(open::Error<'w, PooledWire>)> {
    #[pin]
    #[manifold]
    pub(crate) pool: Listener<'n, 'p, 's, 'scope, 'w, 'd, KeyTag<0>, Tcp, PooledWire, F>,
}
