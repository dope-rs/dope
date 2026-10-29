use std::{convert, mem, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    io::recv,
    random::Seed,
};
use dope_manifold::{
    app::receive::{Borrowed, Outcome},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{
        batch::{self},
        mode,
    },
    send::{
        Completion, Context, Payload, Plain, Policy, Prepared, Storage, Transition, Vectored,
        reclaim,
    },
    terminal,
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::buffer::{
    bytes::TryRetain,
    resident,
    write::{Sink as _, consume},
};
use wire::receive::{eof, mode::access, reserve};

use crate::common::{Buffer, contract::batch::Pair, manifold::listener::finish::close::buffer};

pub const OUTPUT: &[u8] = b"<<CONTROL>>";

pub struct Control {
    pub(crate) pending: Pending,
    faults: Rc<Trace<consume::Error, 16>>,
}

impl Control {
    fn flush<'a>(
        &mut self,
        mut send: Storage<'a, Buffer<64>>,
    ) -> Prepared<'a, reclaim::OnSubmit, Plain<'a>> {
        if let Pending::Output = mem::take(&mut self.pending)
            && let Err(error) = send.bytes.write_slice(OUTPUT)
        {
            self.faults.record(error);
        }
        send.buffered()
    }
}

#[derive(Default)]
pub(crate) enum Pending {
    #[default]
    Clean,
    Output,
}

pub struct Recv;

pub struct Chunks<'input> {
    pub(crate) items: [&'input [u8]; 2],
}

// SAFETY: this policy's Pair batch requires a minimum capacity of two.
// Chunks owns exactly two items and preserves that count when constructing Pair.
unsafe impl<'input> receive::raw::Producer<'input, Control> for Chunks<'input> {
    fn produce(self, _: &batch::Capacity<'_, Control>) -> Pair<&'input [u8]> {
        Pair::new(self.items)
    }
}

impl receive::Policy<Control> for Recv {
    type Batch<'a>
        = Pair<&'a [u8]>
    where
        Control: 'a;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        Control: 'w,
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Control>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Control>,
        end: terminal::Both<'_, terminal::Error<'w, Control>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Control: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        wire.pending = Pending::Output;
        let bytes = &*bytes;
        let (left, right) = bytes.split_at(bytes.len() / 2);
        end.output(capacity.produce(Chunks {
            items: [left, right],
        }))
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Control>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Control>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Control: 'w,
        'd: 'w,
        'w: 'a,
    {
        let wire = txn.into_wire();
        wire.pending = Pending::Output;
        end.output(Some(bytes.into_shared()))
    }
}

pub struct Send;

impl Policy<Control> for Send {
    type Reclaim = reclaim::OnSubmit;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Control, Storage<'a, Buffer<64>>>,
        plain: Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Control>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Control: 'd,
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        end.output(buffer::Buffer::new(send, &wire.faults).plain(plain))
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: Context<'a, 'd, ID, Control, Storage<'a, Buffer<64>>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Control>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Control: 'd,
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        end.output(buffer::Buffer::new(send, &wire.faults).vectored(plain))
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Control, Completion<'a, reclaim::OnSubmit, Buffer<64>>>,
        end: terminal::Send<'_, terminal::Error<'d, Control>>,
    ) -> terminal::Processed<Transition<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Control: 'd,
        'd: 'a,
    {
        let done = context.into_state();
        end.output(buffer::Buffer::complete(done))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Control, Completion<'a, reclaim::OnSubmit, Buffer<64>>>,
        _: Option<terminal::Drain<'_, 'd, Control>>,
    ) -> Transition<'a, Self::Reclaim, impl Payload<'a>>
    where
        Control: 'd,
        'd: 'a,
    {
        let done = context.into_state();
        buffer::Buffer::complete(done)
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Control, Storage<'a, Buffer<64>>>,
        end: terminal::Send<'_, terminal::Error<'d, Control>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        Control: 'd,
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        end.output(wire.flush(send))
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Control, Storage<'a, Buffer<64>>>,
        _: Option<terminal::Drain<'_, 'd, Control>>,
    ) -> Prepared<'a, Self::Reclaim, impl Payload<'a>>
    where
        Control: 'd,
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        wire.flush(send)
    }
}

pub struct Runtime(Rc<Trace<consume::Error, 16>>);

impl<'w, 'd: 'w, ID: Identity + 'w> open::Runtime<'w, 'd, Control, ID> for Runtime {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Control, Buffer<64>) -> open::Disposition<T, Control, Buffer<64>>,
    ) -> open::Outcome<C, T, open::Error<'w, Control>> {
        let wire = Control {
            pending: Pending::Clean,
            faults: Rc::clone(&self.0),
        };
        let output = match consume(context, wire, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl wire::Spec for Control {
    type Connection<'d, ID: Identity + 'd>
        = Self
    where
        Self: 'd;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd>
        = Rc<Trace<consume::Error, 16>>
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Runtime
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = Buffer<64>
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Control;
    type Receive = Recv;
    type Send = Send;
}

impl domain::Init<Control> for Control {
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        faults: <Control as wire::Spec>::Binding<'w, ID>,
    ) -> <Control as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Control: 'w,
        'd: 'w,
    {
        Runtime(faults)
    }
}

#[derive(Debug)]
pub enum State<'d> {
    Fresh,
    Seen(clock::Moment<'d>),
}

#[derive(Debug)]
pub enum Event<'a, 'd> {
    Close(&'a State<'d>),
    Send(&'a State<'d>, clock::Moment<'d>),
}

pub struct App<F> {
    pub(crate) close: Gate,
    pub(crate) received: Rc<Trace<u8, 16>>,
    pub(crate) observe: F,
}

impl<'d, ID: Identity, F> Application<'d, ID> for App<F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Unpin,
{
    type Conn = State<'d>;
    type Build = convert::Infallible;
    type Wire = Control;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = Borrowed;
    type Send = send::Run;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State::Fresh)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Control, State<'d>>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Control, State<'d>>
    where
        'd: 'w,
    {
        let this = self.get_mut();
        (this.observe)(Event::Close(conn.state()));
        this.close.hit();
        conn
    }
}

impl<'d, ID: Identity, F> handler::Borrowed<'d, ID> for App<F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Unpin,
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Control, State<'d>>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        *conn.state_mut() = State::Seen(driver.driver_ref().clock().now());
        for &byte in chunk.as_ref() {
            self.received.record(byte);
        }
        Outcome::Continue
    }
}

impl<'d, ID: Identity, F> send::Drive<'d, ID> for App<F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Unpin,
{
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Control, State<'d>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        (self.observe)(Event::Send(conn.state(), driver.driver_ref().clock().now()));
        conn.close_after();
    }
}
