use std::{array, num::NonZero, ops::ControlFlow};

use dope_core::{
    driver::route::{self, Identity, kind},
    io::{event::{batch::Flow, net::recv}, recv::Lease},
};
use dope_net::wire::{
    self, failure, open, receive,
    receive::{batch, eof, mode, reserve},
    send, terminal,
};
use dope_test::harness::signals::{Gate, Progress as _};

pub struct Capture<'d> {
    pub(crate) done: Gate,
    buffered: Option<(route::Erased<'d, kind::Recv>, Lease<'d>)>,
}

impl<'d> Capture<'d> {
    pub fn new(done: Gate) -> Self {
        Self { done, buffered: None }
    }

    pub fn receive(&mut self, done: recv::Completion<'d>) -> (Option<recv::Completion<'d>>, bool) {
        let mut terminal = false;
        let next = done.dispatch(|token, flow, outcome| match outcome {
            recv::Outcome::Data(bytes) => {
                assert_eq!(bytes.as_slice(), b"F");
                assert!(self.buffered.is_none());
                if flow == Flow::More {
                    self.buffered = Some((token, bytes));
                    ControlFlow::Continue(())
                } else {
                    terminal = true;
                    ControlFlow::Break(recv::Outcome::Data(bytes))
                }
            }
            recv::Outcome::Eof if self.buffered.is_some() => {
                assert_eq!(flow, Flow::Last);
                let (original, bytes) = self.buffered.take().expect("native backing");
                assert!(original.matches(token));
                terminal = true;
                ControlFlow::Break(recv::Outcome::Data(bytes))
            }
            outcome => ControlFlow::Break(outcome),
        });
        (next.break_value(), terminal)
    }
}

#[derive(Clone)]
pub struct State {
    pub(crate) closed: Gate,
    pub(crate) dropped: Gate,
}

pub struct Wire;
pub struct Receive;

pub struct Items<'input> {
    items: array::IntoIter<&'input [u8], 2>,
    state: State,
}

// SAFETY: the array owns exactly two borrowed items and reports its remaining length.
unsafe impl<'input> receive::raw::Batch<'input> for Items<'input> {
    type Item = &'input [u8];
    const MAX_ITEMS: NonZero<usize> = NonZero::<usize>::MIN.saturating_add(1);

    fn len(&self) -> usize { self.items.len() }
    fn next(&mut self) -> Option<Self::Item> { self.items.next() }
}

// SAFETY: the policy's mandatory minimum capacity admits both items.
unsafe impl<'input> receive::raw::Producer<'input, Wire> for Items<'input> {
    fn produce(self, _: &batch::Capacity<'_, Wire>) -> Self { self }
}

impl Drop for Items<'_> {
    fn drop(&mut self) {
        assert_eq!(self.items.len(), 1, "second item was not dispatched");
        assert_eq!(self.state.closed.hits(), 1, "actual Entry retired during first item");
        assert_eq!(self.items.as_slice()[0], b"F", "backing outlives Entry retirement");
        self.state.dropped.hit();
    }
}

impl receive::Policy<Wire> for Receive {
    type Batch<'a> = Items<'a>;
    type Cursor<'w, 'd> = dope_core::io::recv::Shared<'d> where 'd: 'w;
    type Eof = eof::Clean;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;
    type Publication = receive::publication::Raw;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Wire>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where 'd: 'w, 'w: 'a + 'input,
    {
        assert_eq!(bytes, b"F");
        end.output(capacity.produce(Items {
            items: [&*bytes, &*bytes].into_iter(),
            state: txn.into_wire().clone(),
        }))
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        _: reserve::Transaction<'a, 'w, ID, Wire>,
        bytes: Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Wire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where 'd: 'w, 'w: 'a,
    {
        end.output(Some(bytes.into_shared()))
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: Identity + 'd> = State;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd> = State;
    type Runtime<'w, 'd, ID: Identity + 'w> = State where 'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Self;
    type Receive = Receive;
    type Send = send::Direct;
}

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for State {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, State, ()) -> open::Disposition<T, State, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        let output = consume(context, self.clone(), ()).into_output(|_, ()| ());
        open::Outcome::Opened(output)
    }
}
