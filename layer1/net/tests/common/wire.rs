use std::{cell::Cell, rc::Rc};

use dope_core::{driver::route::Identity, io::recv};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{batch, eof, mode, mode::access, reserve},
    send, terminal,
};
use o3::{base::marker::ThreadBound, buffer::storage::inline};

#[derive(Clone, Copy)]
pub struct Failing;

impl failure::Errors<failure::Open> for Failing {
    type Error<'d> = usize;
}

impl failure::Errors<failure::Terminal> for Failing {
    type Error<'d> = usize;
}

impl wire::Spec for Failing {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = ();
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Possible;
    type TerminalFailure = failure::Possible;
    type Bootstrap = Self;
    type Receive = receive::Transparent;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Failing, ID> for Failing {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Failing, ()) -> open::Disposition<T, Failing, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Failing>> {
        let output = match consume(context, *self, ()) {
            open::Disposition::Commit(output) => output,
            open::Disposition::Retry { output, wire, send } => {
                *self = wire;
                let () = send;
                output
            }
        };
        open::Outcome::Opened(output)
    }
}

#[derive(Default)]
pub struct Buffer<const CAP: usize>(pub inline::Bytes<CAP, u16>, pub ThreadBound);

impl<const CAP: usize> send::Store for Buffer<CAP> {
    fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    fn release(self) -> send::Availability {
        send::Availability::Unchanged
    }
}

/// The first byte models authenticated final plaintext from a transforming wire.
pub struct Eof {
    input: Rc<Cell<Input>>,
    thread: ThreadBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    Empty,
    Final,
    Unexpected,
}

impl Eof {
    pub fn new(input: Rc<Cell<Input>>) -> Self {
        Self {
            input,
            thread: ThreadBound::NEW,
        }
    }

    fn observe(&self, bytes: &[u8]) {
        if self.input.get() != Input::Unexpected {
            self.input.set(if bytes == b"F" {
                Input::Final
            } else {
                Input::Unexpected
            });
        }
    }
}

impl wire::Spec for Eof {
    type Connection<'d, ID: Identity + 'd> = Self;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = Self;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Self
    where
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Possible;
    type Bootstrap = Self;
    type Receive = Self;
    type Send = send::Direct;
}

impl domain::Init<Eof> for Eof {
    fn runtime<'w, 'd, ID: Identity + 'w>(_: domain::Limits, binding: Self) -> Self
    where
        'd: 'w,
    {
        binding
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Eof, ID> for Eof {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Self, ()) -> open::Disposition<T, Self, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Eof>> {
        let wire = Self {
            input: Rc::clone(&self.input),
            thread: self.thread,
        };
        open::Outcome::Opened(consume(context, wire, ()).into_output(|_, ()| {}))
    }
}

impl receive::Policy<Eof> for Eof {
    type Batch<'a> = batch::One<&'a [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::State;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Enabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Eof>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Eof>,
        end: terminal::Both<'_, terminal::Error<'w, Eof>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        'd: 'w,
        'w: 'a + 'input,
    {
        txn.into_wire().observe(bytes);
        end.eof(capacity.one(&*bytes))
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Eof>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Eof>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        'd: 'w,
        'w: 'a,
    {
        txn.into_wire().observe(bytes.as_slice());
        end.eof(Some(bytes.into_shared()))
    }
}

impl failure::Errors<failure::Terminal> for Eof {
    type Error<'d> = usize;
}

impl eof::Eof<Eof> for Eof {
    fn eof<'w, ID: Identity + 'w>(
        _: &mut Eof,
        end: terminal::Send<'_, terminal::Error<'w, Eof>>,
    ) -> terminal::Processed<()> {
        end.terminal(
            (),
            terminal::Terminal::Failed(terminal::Error::from_cause(7)),
        )
    }
}
