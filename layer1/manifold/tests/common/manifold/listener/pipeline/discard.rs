use std::{
    cell::Cell,
    convert, marker,
    pin::Pin,
    rc::Rc,
    time::{Duration, Instant},
};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome, limits},
    network::listener::{
        conn::{self, deadline::Arm, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::wire::{
    Identity,
    receive::{Chunk, discard, mode::Disabled},
};
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

pub const PREFIX: &[u8] = b"discard-this-prefix";
pub const PAYLOAD: &[u8] = b"keep-this-payload";
pub(crate) const PREFIX_LENGTH: discard::Prefix = discard::Prefix::fixed::<{ PREFIX.len() }>();
pub const REQUEST: &[u8] = b"!";
pub const ACK: &[u8] = b"ack";

pub enum Finish {
    Payload,
    Prefix(Rc<Cell<Option<Instant>>>),
    Eof(Rc<Cell<Option<Instant>>>),
    Timeout(Gate),
}

pub enum Event<'a, 'd> {
    Request(&'a [u8]),
    Payload(&'a [u8]),
    Length(usize),
    Discard(discard::Start),
    Discarded,
    Deadline(Arm<'d>),
    Write(Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>),
}

pub struct Discard<I, F> {
    pub(crate) gate: Gate,
    pub(crate) retired: Gate,
    pub(crate) finish: Finish,
    pub(crate) input: marker::PhantomData<fn() -> I>,
    pub(crate) observe: F,
}

impl<'d, ID: route::Identity, I: receive::Mode<Identity>, F: for<'a, 'b> Fn(Event<'a, 'b>)>
    Application<'d, ID> for Discard<I, F>
{
    type Conn = Vec<u8>;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = resident::Shared<'d>;
    type Input = I;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(Vec::new())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timing(&self) -> handler::Timing {
        match self.finish {
            Finish::Timeout(_) => handler::Timing::timeout(Duration::from_millis(100)),
            Finish::Payload | Finish::Prefix(_) | Finish::Eof(_) => handler::Timing::default(),
        }
    }

    fn timeout(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Vec<u8>>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        if let Finish::Timeout(expired) = &self.as_ref().get_ref().finish {
            expired.hit();
        }
        handler::Timeout::CloseAfter
    }

    fn accept(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Vec<u8>>,
    ) -> Outcome {
        if matches!(self.as_ref().get_ref().finish, Finish::Payload) {
            (self.observe)(Event::Discard(conn.discard(PREFIX_LENGTH)));
        }
        Outcome::Continue
    }

    fn can_receive(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Identity, Vec<u8>>,
    ) -> bool {
        !matches!(self.get_ref().finish, Finish::Payload) && !conn.state().is_empty()
    }

    fn discarded(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Vec<u8>>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
        (self.observe)(Event::Discarded);
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Vec<u8>>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Vec<u8>>
    where
        'd: 'w,
    {
        let this = self.as_ref().get_ref();
        if let Finish::Prefix(closed) | Finish::Eof(closed) = &this.finish {
            closed.set(Some(Instant::now()));
        }
        this.gate.hit();
        conn
    }
}

pub struct Retirement;

impl<'d, ID: route::Identity, I: receive::Mode<Identity>, F: for<'a, 'b> Fn(Event<'a, 'b>)>
    retire::Policy<'d, Discard<I, F>, ID> for Retirement
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Discard<I, F>>,
        _: conn::Retired<'_, '_, 'd, ID, Identity, Vec<u8>>,
    ) {
        app.as_ref().get_ref().retired.hit();
    }
}

impl<'d, ID: route::Identity, I: receive::Mode<Identity>, F: for<'a, 'b> Fn(Event<'a, 'b>)>
    handler::Borrowed<'d, ID> for Discard<I, F>
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Vec<u8>>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.as_ref().get_ref();
        reply(&this.finish, &this.observe, conn, chunk.as_ref(), driver)
    }
}

impl<'w, 'd: 'w, ID: route::Identity, F: for<'a, 'b> Fn(Event<'a, 'b>)> handler::Owned<'w, 'd, ID>
    for Discard<receive::Owned, F>
{
    const RETENTION: limits::Retention = limits::Retention::new(1, 0);

    fn chunk(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Identity, Vec<u8>>,
        chunk: Chunk<'w, 'd, Identity, Disabled>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.as_ref().get_ref();
        reply(&this.finish, &this.observe, conn, chunk.as_ref(), driver)
    }
}

fn reply<'w, 'd: 'w, ID: route::Identity>(
    finish: &Finish,
    observe: &impl for<'a, 'b> Fn(Event<'a, 'b>),
    mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Identity, Vec<u8>>,
    chunk: &[u8],
    driver: &mut retained::Context<'_, '_, 'd>,
) -> Outcome {
    if !matches!(finish, Finish::Payload) {
        if !conn.state().is_empty() {
            return Outcome::Continue;
        }
        observe(Event::Request(chunk));
        conn.state_mut().extend_from_slice(chunk);
        observe(Event::Write(conn.write().try_bounded(
            driver,
            |_, prefix| {
                prefix[..ACK.len()].copy_from_slice(ACK);
                write::Draft::new(ACK.len(), None, ())
            },
        )));
        if matches!(finish, Finish::Timeout(_)) {
            observe(Event::Deadline(
                conn.deadline()
                    .try_ensure(driver.driver_ref().clock().now()),
            ));
        }
        conn.close_after();
        observe(Event::Discard(conn.try_discard(PREFIX_LENGTH)));
        return Outcome::Continue;
    }
    let state = conn.state_mut();
    state.extend_from_slice(chunk);
    observe(Event::Payload(state));
    if state.len() != PAYLOAD.len() {
        return Outcome::Continue;
    }
    observe(Event::Write(conn.write().try_bounded(
        driver,
        |state, prefix| {
            observe(Event::Length(state.len()));
            prefix[..PAYLOAD.len()].copy_from_slice(PAYLOAD);
            write::Draft::new(PAYLOAD.len(), None, ())
        },
    )));
    Outcome::CloseAfter
}
