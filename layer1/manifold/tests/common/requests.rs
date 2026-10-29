use std::{cell::Cell, convert::Infallible, marker::PhantomData, pin::Pin, ptr};

use dope_core::{
    driver::{
        receive::credit,
        retained,
        route::{Identity, table::conn::Capacity},
        schedule::clock,
    },
    io::{recv, socket},
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, limits::Budget},
    network::listener::{
        conn,
        handler::{self, failures, retire, send},
    },
};
use dope_net::wire::{
    self, domain, failure, open,
    receive::{Policy, batch, eof, mode, mode::access, reserve, scoped},
    send::Direct,
    terminal,
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{
    base::Call,
    buffer::{bytes::TryRetain, hold::limits, resident},
    depot::queue::tagged::entry,
};

use crate::common::contract::batch::Pair;

pub struct State {
    pub(crate) source: u8,
    pub(crate) retry: bool,
    pub(crate) reservations: Cell<usize>,
    pub(crate) registered: Cell<usize>,
    pub(crate) cancelled: Cell<usize>,
    pub(crate) events: Trace<Event, 1>,
    pub(crate) closed: Gate,
}

impl State {
    pub fn new(retry: bool) -> Self {
        Self {
            source: 37,
            retry,
            reservations: Cell::new(0),
            registered: Cell::new(0),
            cancelled: Cell::new(0),
            events: Trace::new(),
            closed: Gate::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Event {
    RecvCredit,
    SendCredit,
    Peer(socket::Error),
    Chunk,
    Wire(*const u8, Option<*const u8>),
}

#[derive(Debug)]
pub struct Cause<'w>(pub(crate) &'w u8);

pub struct Wire;
pub struct Receive;
pub enum Transaction {}
pub struct Wait<'a>(&'a State);
pub struct Registration<'d, ID: Identity>(credit::Pending<'d, ID>);

impl reserve::Completion for Transaction {
    fn complete(self) {
        match self {}
    }
}

impl<ID: Identity> reserve::Registration for Registration<'_, ID> {
    fn commit(self) {
        self.0.claim().retry();
    }
}

impl<'d, ID: Identity> reserve::Wait<'d, ID, Wire> for Wait<'_> {
    type Registration = Registration<'d, ID>;

    fn register(
        self,
        pending: credit::Pending<'d, ID>,
    ) -> Result<Self::Registration, entry::Error<Capacity>> {
        self.0.registered.set(self.0.registered.get() + 1);
        Ok(Registration(pending))
    }
}

impl reserve::Try<Wire> for Receive {
    type Block<'a, 'w, 'd, ID: Identity + 'w>
        = Wait<'a>
    where
        'd: 'w,
        'w: 'a;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
        = Transaction
    where
        'd: 'w,
        'w: 'a;

    fn try_reserve<'a, 'w, 'd, ID: Identity + 'w>(
        _: &'a mut wire::Identity,
        _: &'a mut (),
        state: &'a mut &'w State,
    ) -> reserve::Outcome<Transaction, Wait<'a>, terminal::Error<'w, Wire>>
    where
        'd: 'w,
        'w: 'a,
    {
        let state = *state;
        let first = state.reservations.replace(state.reservations.get() + 1) == 0;
        if first && state.retry {
            reserve::Outcome::Blocked(Wait(state))
        } else {
            reserve::Outcome::Failed(terminal::Error::from_cause(Cause(&state.source)))
        }
    }

    fn cancel<'w, 'd, ID: Identity + 'w>(state: &mut &'w State, _: credit::Id<'d, ID>)
    where
        'd: 'w,
    {
        state.cancelled.set(state.cancelled.get() + 1);
    }

    fn release_recv<'w, 'd, ID: Identity + 'w>(state: &mut &'w State)
    where
        'd: 'w,
    {
        state.events.record(Event::RecvCredit);
    }

    fn release_send<'w, 'd, ID: Identity + 'w>(state: &mut &'w State)
    where
        'd: 'w,
    {
        state.events.record(Event::SendCredit);
    }
}

impl Policy<Wire> for Receive {
    type Batch<'input> = Pair<&'input [u8]>;
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
        txn: Transaction,
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
        txn: Transaction,
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

impl failure::Errors<failure::Terminal> for Wire {
    type Error<'w> = Cause<'w>;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for &'w State {
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
    type Connection<'w, ID: Identity + 'w> = wire::Identity;
    type Storage<ID: Identity> = ();
    type Binding<'w, ID: Identity + 'w> = &'w State;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = &'w State
    where
        'd: 'w;
    type SendStorage<'w> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;
    type Bootstrap = Self;
    type Receive = Receive;
    type Send = Direct;
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(_: domain::Limits, state: &'w State) -> &'w State
    where
        'd: 'w,
    {
        state
    }
}

pub struct App<'a, I> {
    state: &'a State,
    input: PhantomData<fn() -> I>,
}

impl<'a, I> App<'a, I> {
    pub fn new(state: &'a State) -> Self {
        Self {
            state,
            input: PhantomData,
        }
    }
}

pub struct Failure;

impl<'d, ID: Identity, I: receive::Mode<Wire>> handler::Application<'d, ID> for App<'_, I> {
    type Build = Infallible;
    type Conn = ();
    type Wire = Wire;
    type WireFailure = Failure;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = I;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Infallible) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<(), Infallible> {
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, ()>
    where
        'd: 'w,
    {
        self.state.closed.hit();
        conn
    }
}

impl<'state, 'w, 'd: 'w, ID: Identity, I: receive::Mode<Wire>>
    failures::Policy<'w, 'd, App<'state, I>, ID> for Failure
{
    fn open_failed(_: Pin<&mut App<'_, I>>, cause: open::Error<'w, Wire>) {
        cause.eliminate()
    }

    fn peer() -> Option<impl for<'a> Call<(Pin<&'a mut App<'state, I>>, socket::Error)>> {
        Some(|(app, cause): (Pin<&mut App<'state, I>>, socket::Error)| {
            app.state.events.record(Event::Peer(cause));
        })
    }

    fn wire_failed<'e>(
        app: Pin<&mut App<'_, I>>,
        _: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Wire, ()>,
        cause: terminal::Error<'w, Wire>,
    ) {
        let (cause, closing) = cause.into_parts();
        app.state.events.record(Event::Wire(
            ptr::from_ref(cause.0),
            closing.map(|cause| ptr::from_ref(cause.0)),
        ));
    }
}

impl<'d, ID: Identity> handler::Borrowed<'d, ID> for App<'_, receive::Borrowed> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, ()>,
        _: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        self.state.events.record(Event::Chunk);
        receive::Outcome::CloseAfter
    }
}

impl<'w, 'd: 'w, ID: Identity> handler::Scoped<'w, 'd, ID> for App<'_, receive::Scoped> {
    fn retention(&self) -> Budget {
        Budget::new(limits::Bound::new(0), 4096)
    }

    fn scoped_chunk(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Wire, ()>,
        _: scoped::Input<'_, 'w, 'd, Wire>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        self.state.events.record(Event::Chunk);
        receive::Outcome::CloseAfter
    }
}
