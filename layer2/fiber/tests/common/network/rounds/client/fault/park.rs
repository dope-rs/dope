use std::cell::Cell;

use dope::{
    core::{
        driver::{
            receive::credit,
            route::{Identity, table::conn::Capacity},
        },
        io::recv,
    },
    net::wire::{
        self, domain, failure, open,
        receive::{self, batch, eof, mode, mode::access, reserve},
        send, terminal,
    },
};
use dope_test::harness::signals::LocalGate;
use o3::depot::queue::tagged::entry;
use reserve::Completion as _;

pub struct Wire;
pub struct Receive;
pub enum Never {}

#[derive(Default)]
pub struct State {
    pub received: LocalGate,
    pub rejected: LocalGate,
    pub released: LocalGate,
    pub cancelled: Cell<Option<u32>>,
}

pub struct Conn<'a> {
    state: &'a State,
    received: bool,
}

pub struct Transaction<'a, 'w>(&'a mut Conn<'w>);
pub struct Wait<'a>(&'a State);

impl reserve::Completion for Transaction<'_, '_> {
    fn complete(self) {
        self.0.received = true;
        self.0.state.received.handle().hit();
    }
}

impl reserve::Registration for Never {
    fn commit(self) {
        match self {}
    }
}

impl<'d, ID: Identity> reserve::Wait<'d, ID, Wire> for Wait<'_> {
    type Registration = Never;

    fn register(self, _: credit::Pending<'d, ID>) -> Result<Never, entry::Error<Capacity>> {
        self.0.rejected.handle().hit();
        Err(entry::Error::Index {
            index: 1,
            capacity: Capacity::fixed::<1>(),
        })
    }
}

impl reserve::Try<Wire> for Receive {
    type Block<'a, 'w, 'd, ID: Identity + 'w>
        = Wait<'w>
    where
        'd: 'w,
        'w: 'a;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
        = Transaction<'a, 'w>
    where
        'd: 'w,
        'w: 'a;

    fn try_reserve<'a, 'w, 'd, ID: Identity + 'w>(
        wire: &'a mut Conn<'w>,
        _: &'a mut (),
        _: &'a mut &'w State,
    ) -> reserve::Outcome<Transaction<'a, 'w>, Wait<'w>, terminal::Error<'w, Wire>>
    where
        'd: 'w,
        'w: 'a,
    {
        if wire.received {
            reserve::Outcome::Blocked(Wait(wire.state))
        } else {
            reserve::Outcome::Ready(Transaction(wire))
        }
    }

    fn cancel<'w, 'd, ID: Identity + 'w>(run: &mut &'w State, target: credit::Id<'d, ID>)
    where
        'd: 'w,
    {
        run.cancelled.set(Some(target.slot().index().get()));
    }

    fn release_recv<'w, 'd, ID: Identity + 'w>(run: &mut &'w State)
    where
        'd: 'w,
    {
        run.released.handle().hit();
    }

    fn release_send<'w, 'd, ID: Identity + 'w>(run: &mut &'w State)
    where
        'd: 'w,
    {
        run.released.handle().hit();
    }
}

impl receive::Policy<Wire> for Receive {
    type Batch<'a> = batch::One<&'a [u8]>;
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
        txn: Transaction<'a, 'w>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        'd: 'w,
        'w: 'a + 'input,
    {
        txn.complete();
        end.output(capacity.one(&*bytes))
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: Transaction<'a, 'w>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        'd: 'w,
        'w: 'a,
    {
        txn.complete();
        end.output(Some(bytes.into_shared()))
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = Conn<'d>;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = &'d State;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = &'w State
    where
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Self;
    type Receive = Receive;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for &'w State {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Conn<'w>, ()) -> open::Disposition<T, Conn<'w>, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let wire = Conn {
            state: self,
            received: false,
        };
        let output = match consume(context, wire, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl domain::Init<Wire> for Wire {
    fn runtime<'w, 'd, ID: Identity + 'w>(_: domain::Limits, state: &'w State) -> &'w State
    where
        'd: 'w,
    {
        state
    }
}
