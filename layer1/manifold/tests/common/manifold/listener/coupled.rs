use std::{convert, mem, num, pin::Pin, rc::Rc};

use conn::write::{self, error};
use dope_core::{
    driver::{receive::credit, retained, route, schedule::clock},
    io::recv,
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome, limits::Budget},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::egress::{self, load::Value},
    wire::{self, Identity, domain, failure, open, receive as input, receive::scoped, terminal},
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{
    alloc,
    buffer::{
        bytes::TryRetain,
        hold::{limits, scope},
        resident, segments,
        storage::{self, Shared},
        write::consume,
    },
    depot::queue::tagged::entry,
};

pub const BODY_LEN: usize = 1024 * 1024;
pub(crate) static QUEUED_BODY: [u8; BODY_LEN] = [0x6b; BODY_LEN];

pub enum State {
    First(egress::NonEmpty<segments::Chain<Shared>>),
    Second,
    Done,
}

pub enum Event<'a> {
    Prefix(&'a State, usize),
    Written(
        Result<write::Outcome<()>, error::Error<segments::Chain<Shared>, (), convert::Infallible>>,
    ),
}

pub struct App<F> {
    pub(crate) state: State,
    pub(crate) opened: Gate,
    pub(crate) first: Gate,
    pub(crate) second: Gate,
    pub(crate) closed: Gate,
    pub(crate) seen: Rc<Trace<Vec<u8>, 16>>,
    pub(crate) observe: F,
}

impl<'d, ID: route::Identity, F: for<'a> Fn(Event<'a>) + Unpin> Application<'d, ID> for App<F> {
    type Build = convert::Infallible;
    type Conn = ();
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = segments::Chain<Shared>;
    type Input = receive::Coupled;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        self.opened.hit();
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, (), Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, (), Self::Egress>
    where
        'd: 'w,
    {
        self.get_mut().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(Event<'a>) + Unpin> handler::Borrowed<'d, ID>
    for App<F>
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, (), Self::Egress>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.seen.record(chunk.as_ref().to_vec());
        match mem::replace(&mut this.state, State::Done) {
            State::First(body) => {
                this.state = State::Second;
                let body = Value::Checked(body);
                let done = conn.write().try_bounded(driver, |_, prefix| {
                    (this.observe)(Event::Prefix(&this.state, prefix.len()));
                    write::Draft::new(0, Some(body), ())
                });
                (this.observe)(Event::Written(done));
                this.first.hit();
            }
            State::Second => {
                conn.close_after();
                this.second.hit();
            }
            State::Done => return Outcome::CloseAfter,
        }
        Outcome::Continue
    }
}

#[derive(Debug)]
pub enum BuildError {
    Pool(alloc::Error),
    Owner(storage::AllocError),
    Segments(segments::Error),
}

impl State {
    pub fn try_body() -> Result<segments::Chain<Shared>, BuildError> {
        let pool = segments::Pool::try_new(segments::Plan {
            segments: BODY_LEN / 4096,
            chains: 1,
        })
        .map_err(BuildError::Pool)?;
        let mut chain = pool.into_chain();
        for _ in 0..BODY_LEN / 4096 {
            let owner = Shared::try_copy(&[0x5a; 4096]).map_err(BuildError::Owner)?;
            chain
                .try_push(owner)
                .map_err(|error| BuildError::Segments(error.into_parts().1))?;
        }
        Ok(chain)
    }
}

#[derive(Default)]
pub struct DynamicState {
    pub(crate) phase: u8,
    pub(crate) prefix_completions: usize,
}

pub enum DynamicEvent<'d> {
    Chunk(u8, usize),
    Consumed(Option<Result<(), consume::scope::Error>>),
    Prefix(u8, usize),
    Written(
        Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>,
    ),
    Drained(u8),
}

pub struct DynamicApp<F> {
    pub(crate) first: Gate,
    pub(crate) prefix: Gate,
    pub(crate) drained: Gate,
    pub(crate) second: Gate,
    pub(crate) closed: Gate,
    pub(crate) seen: Rc<Trace<Vec<u8>, 16>>,
    pub(crate) observe: F,
}

impl<'d, ID: route::Identity, F: for<'a> Fn(DynamicEvent<'a>) + Unpin> Application<'d, ID>
    for DynamicApp<F>
{
    type Build = convert::Infallible;
    type Conn = DynamicState;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::GatedScoped;
    type Send = send::Ready;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(DynamicState::default())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn is_blocked(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Identity, DynamicState>,
    ) -> bool {
        conn.state().phase == 1
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, DynamicState>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, DynamicState>
    where
        'd: 'w,
    {
        self.get_mut().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(DynamicEvent<'a>) + Unpin> send::Notify<'d, ID>
    for DynamicApp<F>
{
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, DynamicState>,
    ) {
        let this = self.get_mut();
        conn.state_mut().prefix_completions += 1;
        this.prefix.hit();
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(DynamicEvent<'a>) + Unpin> handler::Gated<'d, ID>
    for DynamicApp<F>
{
    fn send_drained(
        self: Pin<&mut Self>,
        mut conn: conn::Drained<'_, '_, '_, '_, '_, 'd, ID, Identity, DynamicState>,
    ) -> handler::PostDrain {
        (self.observe)(DynamicEvent::Drained(conn.state().phase));
        conn.state_mut().phase = 2;
        self.get_mut().drained.hit();
        handler::PostDrain::Idle
    }
}

impl<'w, 'd: 'w, ID: route::Identity, F: for<'a> Fn(DynamicEvent<'a>) + Unpin>
    handler::Scoped<'w, 'd, ID> for DynamicApp<F>
{
    fn retention(&self) -> Budget {
        Budget::new(limits::Bound::new(0), 4096)
    }

    fn scoped_chunk(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Identity, DynamicState>,
        chunk: scoped::Input<'_, 'w, 'd, Identity>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.seen.record(chunk.as_ref().to_vec());
        (this.observe)(DynamicEvent::Chunk(
            conn.state().phase,
            conn.state().prefix_completions,
        ));
        match conn.state().phase {
            0 => {
                conn.state_mut().phase = 1;
                let body = egress::NonEmpty::from_array(&QUEUED_BODY);
                let body = Value::Checked(body);
                let done = conn.write().try_bounded(driver, |state, prefix| {
                    (this.observe)(DynamicEvent::Prefix(state.phase, prefix.len()));
                    write::Draft::new(0, Some(body), ())
                });
                (this.observe)(DynamicEvent::Written(done));
                this.first.hit();
                return Outcome::Continue;
            }
            2 => {
                conn.state_mut().phase = 3;
            }
            3 => {
                conn.state_mut().phase = 4;
                conn.close_after();
                this.second.hit();
            }
            _ => return Outcome::CloseAfter,
        }
        let (chunk, budget) = chunk.into_parts();
        let consumed = scope::Input::consume(chunk, budget, |input| {
            let Some(len) = num::NonZero::new(input.as_slice().len()) else {
                return (None, None);
            };
            match input.try_prefix::<{ u32::MAX }>(len) {
                Ok((_, end)) => (Some(end), Some(Ok(()))),
                Err(error) => (None, Some(Err(error))),
            }
        });
        (this.observe)(DynamicEvent::Consumed(consumed));
        Outcome::Continue
    }
}

pub struct BorrowedWire(pub Rc<Trace<bool, 16>>);
pub struct Reserved;
pub enum Blocked {}

impl input::reserve::Completion for Reserved {
    fn complete(self) {}
}

impl input::reserve::Registration for Blocked {
    fn commit(self) {
        match self {}
    }
}

impl<'d, ID: route::Identity> input::reserve::Wait<'d, ID, BorrowedWire> for Blocked {
    type Registration = Self;

    fn register(
        self,
        _: credit::Pending<'d, ID>,
    ) -> Result<Self, entry::Error<route::table::conn::Capacity>> {
        match self {}
    }
}

impl input::reserve::Try<BorrowedWire> for BorrowedWire {
    type Block<'a, 'w, 'd, ID: route::Identity + 'w>
        = Blocked
    where
        'd: 'w,
        'w: 'a;
    type Transaction<'a, 'w, 'd, ID: route::Identity + 'w>
        = Reserved
    where
        'd: 'w,
        'w: 'a;

    fn try_reserve<'a, 'w, 'd, ID: route::Identity + 'w>(
        _: &'a mut Identity,
        send: Option<&'a mut ()>,
        run: &'a mut BorrowedWire,
    ) -> input::reserve::Outcome<Reserved, Blocked, terminal::Error<'w, BorrowedWire>>
    where
        'd: 'w,
        'w: 'a,
    {
        run.0.record(send.is_some());
        input::reserve::Outcome::Ready(Reserved)
    }

    fn cancel<'w, 'd: 'w, ID: route::Identity + 'w>(_: &mut BorrowedWire, _: credit::Id<'d, ID>) {}

    fn release_recv<'w, 'd: 'w, ID: route::Identity + 'w>(_: &mut BorrowedWire) {}

    fn release_send<'w, 'd: 'w, ID: route::Identity + 'w>(_: &mut BorrowedWire) {}
}

impl input::Policy<BorrowedWire> for BorrowedWire {
    type Batch<'input> = input::batch::One<&'input [u8]>;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        'd: 'w;
    type Access = input::mode::access::Direct;
    type Eof = input::eof::Clean;
    type Reserve = input::reserve::Waiting<Self>;
    type Credit = input::mode::Disabled;
    type Discard = input::mode::Disabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: route::Identity + 'w>(
        txn: Reserved,
        bytes: &'input mut [u8],
        capacity: input::batch::Grant<'id, 'd, BorrowedWire>,
        end: terminal::Both<'_, terminal::Error<'w, BorrowedWire>>,
    ) -> terminal::Processed<input::batch::Produced<'id, Self::Batch<'input>>>
    where
        'd: 'w,
        'w: 'a + 'input,
    {
        input::reserve::Completion::complete(txn);
        end.output(capacity.one(&*bytes))
    }

    fn read_lease<'a, 'w, 'd, ID: route::Identity + 'w>(
        txn: Reserved,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, BorrowedWire>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        'd: 'w,
        'w: 'a,
    {
        input::reserve::Completion::complete(txn);
        end.output(Some(bytes.into_shared()))
    }
}

impl<'w, 'd: 'w, ID: route::Identity + 'w> open::Runtime<'w, 'd, BorrowedWire, ID>
    for BorrowedWire
{
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Identity, ()) -> open::Disposition<T, Identity, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, BorrowedWire>> {
        let output = match consume(context, Identity, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl wire::Spec for BorrowedWire {
    type Connection<'w, ID: route::Identity + 'w> = Identity;
    type Storage<ID: route::Identity> = ();
    type Binding<'w, ID: route::Identity + 'w> = Self;
    type Runtime<'w, 'd, ID: route::Identity + 'w>
        = Self
    where
        'd: 'w;
    type SendStorage<'w> = ();
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;
    type Bootstrap = Self;
    type Receive = Self;
    type Send = wire::send::Direct;
}

impl domain::Init<BorrowedWire> for BorrowedWire {
    fn runtime<'w, 'd: 'w, ID: route::Identity + 'w>(_: domain::Limits, binding: Self) -> Self {
        binding
    }
}

pub struct BorrowedApp {
    pub(crate) first: Gate,
    pub(crate) second: Gate,
    pub(crate) sent: Gate,
    pub(crate) closed: Gate,
    pub(crate) seen: Rc<Trace<u8, 16>>,
    pub(crate) written: Rc<Trace<bool, 1>>,
    pub(crate) released: Rc<Trace<bool, 1>>,
}

impl<'d, ID: route::Identity> Application<'d, ID> for BorrowedApp {
    type Build = convert::Infallible;
    type Conn = ();
    type Wire = BorrowedWire;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Reported<send::Silent>;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, BorrowedWire, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, BorrowedWire, ()>
    where
        'd: 'w,
    {
        self.closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for BorrowedApp {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, BorrowedWire, ()>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        for &byte in chunk.as_ref() {
            self.seen.record(byte);
        }
        match chunk.as_ref() {
            b"A" => {
                let body = Value::Checked(egress::NonEmpty::from_array(&QUEUED_BODY));
                let done = conn
                    .write()
                    .try_bounded(driver, |_, _| write::Draft::new(0, Some(body), ()));
                self.written
                    .record(matches!(done, Ok(write::Outcome::Committed(()))));
                self.first.hit();
            }
            b"B" => self.second.hit(),
            _ => return Outcome::CloseAfter,
        }
        Outcome::Continue
    }
}

impl<'d, ID: route::Identity> send::Report<'d, ID> for BorrowedApp {
    const ENABLED: bool = true;

    fn released(self: Pin<&mut Self>, release: handler::Release<'d, ID, Self::Egress>) {
        let (_, _, cause) = release.into_parts();
        self.released.record(cause == handler::Disposition::Sent);
        self.sent.hit();
    }
}
