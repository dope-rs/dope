use std::{convert, pin::Pin};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    io::recv,
    random::Seed,
};
use dope_manifold::{
    app::receive::{Borrowed, End, Outcome},
    network::listener::{
        conn::{self, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::wire::{
    self, domain, failure, open, receive,
    receive::{batch, mode},
    send::{
        Completion, Context, Payload, Plain, Policy, Prepared, Storage, Transition, Vectored,
        progress::Plan, reclaim,
    },
    terminal,
};
use dope_test::harness::signals::Gate;
use o3::buffer::{
    bytes::TryRetain,
    resident,
    write::{
        Sink as _,
        consume::{self, Consume as _},
    },
};
use wire::receive::{eof, mode::access, reserve};

use crate::common::Buffer;

pub const HANDSHAKE: &[u8] = b"hellohello";
pub const PREFIX: u8 = 0xa5;
pub const FRAMES: &[&[u8]] = &[b"PREAMBLE", b"SETTINGS"];

pub enum Event<'a, 'd> {
    Write(Result<(), consume::Error>),
    Input {
        expected: &'a [u8],
        actual: &'a [u8],
    },
    Consumed {
        expected: usize,
        actual: usize,
    },
    Enqueued(
        Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>,
    ),
    Chunk(&'a [u8]),
    Eof(End),
}

pub struct Preamble<F> {
    pub(crate) pending: usize,
    eof_on_ready: bool,
    observe: F,
}

impl<F> Preamble<F>
where
    F: for<'a, 'd> Fn(Event<'a, 'd>) + Clone + Unpin,
{
    pub(crate) fn is_ready(&self) -> bool {
        self.pending == HANDSHAKE.len()
    }

    fn finish<'d, T>(
        &self,
        output: T,
        end: terminal::Both<'_, terminal::Error<'d, Self>>,
    ) -> terminal::Processed<T>
    where
        Self: 'd,
    {
        if self.eof_on_ready && self.is_ready() {
            end.eof(output)
        } else {
            end.output(output)
        }
    }

    pub(crate) fn prepare<'a>(
        &self,
        send: Storage<'a, Buffer<1024>>,
        plain: Plain<'a>,
    ) -> Prepared<'a, reclaim::OnSubmit, Plain<'a>> {
        let len = plain.len();
        if self.is_ready() && send.bytes.is_empty() && send.bytes.available() > len.into_raw() {
            let mut plan = Plan::plain(send, plain);
            let (store, mut input) = plan.parts();
            let part = input.take(len);
            (self.observe)(Event::Write(store.bytes.write_slice(&[PREFIX])));
            (self.observe)(Event::Write(store.bytes.write_slice(part.as_slice())));
            part.commit();
            return plan.finish().buffered();
        }
        send.consume_none().buffered()
    }
}

pub struct Recv;

pub struct Runtime<F>(F, bool);

impl<'w, 'd: 'w, ID: Identity + 'w, F> open::Runtime<'w, 'd, Preamble<F>, ID> for Runtime<F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Clone + Unpin + 'w,
{
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(
            C,
            Preamble<F>,
            Buffer<1024>,
        ) -> open::Disposition<T, Preamble<F>, Buffer<1024>>,
    ) -> open::Outcome<C, T, open::Error<'w, Preamble<F>>> {
        let wire = Preamble {
            pending: 0,
            eof_on_ready: self.1,
            observe: self.0.clone(),
        };
        let output = match consume(context, wire, Buffer::default()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl<F> receive::Policy<Preamble<F>> for Recv
where
    F: for<'a, 'd> Fn(Event<'a, 'd>) + Clone + Unpin,
{
    type Batch<'a>
        = batch::One<&'a [u8]>
    where
        Preamble<F>: 'a;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        Preamble<F>: 'w,
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Borrowed;
    type Credit = mode::Disabled;
    type Discard = mode::Enabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Preamble<F>>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Preamble<F>>,
        end: terminal::Both<'_, terminal::Error<'w, Preamble<F>>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Preamble<F>: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let wire = txn.into_wire();
        let count = (HANDSHAKE.len() - wire.pending).min(bytes.len());
        (wire.observe)(Event::Input {
            expected: &HANDSHAKE[wire.pending..][..count],
            actual: &bytes[..count],
        });
        wire.pending += count;
        let bytes = &bytes[count..];
        if bytes.is_empty() {
            return wire.finish(capacity.maybe(None), end);
        }
        wire.finish(capacity.maybe(Some(bytes)), end)
    }

    fn read_lease<'a, 'w, 'd, ID: Identity + 'w>(
        txn: reserve::Transaction<'a, 'w, ID, Preamble<F>>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Preamble<F>>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Preamble<F>: 'w,
        'd: 'w,
        'w: 'a,
    {
        let mut bytes = bytes;
        let wire = txn.into_wire();
        let count = (HANDSHAKE.len() - wire.pending).min(bytes.as_ref().len());
        (wire.observe)(Event::Input {
            expected: &HANDSHAKE[wire.pending..][..count],
            actual: &bytes.as_ref()[..count],
        });
        (wire.observe)(Event::Consumed {
            expected: count,
            actual: bytes.consume_max(count),
        });
        wire.pending += count;
        if bytes.as_ref().is_empty() {
            return wire.finish(None, end);
        }
        wire.finish(Some(bytes.into_shared()), end)
    }
}

pub struct Send;

impl<F> Policy<Preamble<F>> for Send
where
    F: for<'a, 'd> Fn(Event<'a, 'd>) + Clone + Unpin,
{
    type Reclaim = reclaim::OnSubmit;

    fn prepare<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Preamble<F>, Storage<'a, Buffer<1024>>>,
        plain: Plain<'a>,
        end: terminal::Send<'_, terminal::Error<'d, Preamble<F>>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        end.output(wire.prepare(send, plain))
    }

    fn prepare_many<'a, 'd, ID: Identity + 'd, const N: usize>(
        context: Context<'a, 'd, ID, Preamble<F>, Storage<'a, Buffer<1024>>>,
        plain: Vectored<'a, N>,
        end: terminal::Send<'_, terminal::Error<'d, Preamble<F>>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let (wire, send) = context.into_parts();
        let used = plain.bytes();
        if !wire.is_ready() || !send.bytes.is_empty() || send.bytes.available() <= used.into_raw() {
            return end.output(send.consume_none().buffered());
        }
        let mut plan = Plan::vectored(send, &plain);
        let (store, mut input) = plan.parts();
        let mut parts = input.take(used.min(input.remaining()));
        (wire.observe)(Event::Write(store.bytes.write_slice(&[PREFIX])));
        for part in parts.take() {
            (wire.observe)(Event::Write(store.bytes.write_slice(part.bytes())));
        }
        parts.commit();
        end.output(plan.finish().buffered())
    }

    fn after_send<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Preamble<F>, Completion<'a, reclaim::OnSubmit, Buffer<1024>>>,
        end: terminal::Send<'_, terminal::Error<'d, Preamble<F>>>,
    ) -> terminal::Processed<Transition<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        let send = done.consume_max();
        end.output(Transition::unchanged(send.consume_none().buffered()))
    }

    fn settle<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Preamble<F>, Completion<'a, reclaim::OnSubmit, Buffer<1024>>>,
        _: Option<terminal::Drain<'_, 'd, Preamble<F>>>,
    ) -> Transition<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let done = context.into_state();
        let send = done.consume_max();
        Transition::unchanged(send.consume_none().buffered())
    }

    fn flush<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Preamble<F>, Storage<'a, Buffer<1024>>>,
        end: terminal::Send<'_, terminal::Error<'d, Preamble<F>>>,
    ) -> terminal::Processed<Prepared<'a, Self::Reclaim, impl Payload<'a>>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        end.output(send.consume_none().buffered())
    }

    fn drain<'a, 'd, ID: Identity + 'd>(
        context: Context<'a, 'd, ID, Preamble<F>, Storage<'a, Buffer<1024>>>,
        _: Option<terminal::Drain<'_, 'd, Preamble<F>>>,
    ) -> Prepared<'a, Self::Reclaim, impl Payload<'a>>
    where
        'd: 'a,
    {
        let send = context.into_state();
        send.consume_none().buffered()
    }
}

impl<F> wire::Spec for Preamble<F>
where
    F: for<'a, 'd> Fn(Event<'a, 'd>) + Clone + Unpin,
{
    type Connection<'d, ID: Identity + 'd>
        = Self
    where
        Self: 'd;
    type Storage<ID: Identity> = ();
    type Binding<'d, ID: Identity + 'd>
        = (F, bool)
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: Identity + 'w>
        = Runtime<F>
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = Buffer<1024>
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Self;
    type Receive = Recv;
    type Send = Send;
}

impl<F> domain::Init<Preamble<F>> for Preamble<F>
where
    F: for<'a, 'd> Fn(Event<'a, 'd>) + Clone + Unpin,
{
    fn runtime<'w, 'd, ID: Identity + 'w>(
        _: domain::Limits,
        observe: <Preamble<F> as wire::Spec>::Binding<'w, ID>,
    ) -> <Preamble<F> as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Preamble<F>: 'w,
        'd: 'w,
    {
        Runtime(observe.0, observe.1)
    }
}

pub struct App<'a, F> {
    pub(crate) frames: &'a [&'a [u8]],
    pub(crate) gate: Gate,
    pub(crate) observe: F,
}

impl<'d, ID: Identity, F> Application<'d, ID> for App<'_, F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Clone + Unpin + 'd,
{
    type Conn = u8;
    type Build = convert::Infallible;
    type Wire = Preamble<F>;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = Borrowed;
    type Send = send::Run;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(0)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Preamble<F>, u8>,
    ) -> Outcome {
        conn.wake_target().wake();
        Outcome::Continue
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Preamble<F>, u8>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Preamble<F>, u8>
    where
        'd: 'w,
    {
        let this = self.get_mut();
        for frame in this.frames.iter().skip(usize::from(*conn.state())) {
            let result = conn.write().try_bounded(driver, |state, prefix| {
                prefix[..frame.len()].copy_from_slice(frame);
                *state += 1;
                write::Draft::new(frame.len(), None, ())
            });
            (this.observe)(Event::Enqueued(result));
        }
        conn
    }

    fn eof(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Preamble<F>, u8>,
        end: End,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Eof {
        (self.observe)(Event::Eof(end));
        handler::Eof::Close
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Preamble<F>, u8>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Preamble<F>, u8>
    where
        'd: 'w,
    {
        self.get_mut().gate.hit();
        conn
    }
}

impl<'d, ID: Identity, F> send::Drive<'d, ID> for App<'_, F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Clone + Unpin + 'd,
{
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Preamble<F>, u8>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        let sent = conn.state_mut();
        if usize::from(*sent) == self.frames.len() {
            conn.close_after();
        }
    }
}

impl<'d, ID: Identity, F> handler::Borrowed<'d, ID> for App<'_, F>
where
    F: for<'a, 'scope> Fn(Event<'a, 'scope>) + Clone + Unpin + 'd,
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Preamble<F>, u8>,
        chunk: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref()));
        Outcome::Continue
    }
}
