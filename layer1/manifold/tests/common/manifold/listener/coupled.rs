use std::{convert, mem, pin::Pin, rc::Rc};

use conn::write::{self, error};
use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::egress::{self, load::Value},
    wire::Identity,
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{
    alloc,
    buffer::{
        bytes::TryRetain,
        resident, segments,
        storage::{self, Shared},
    },
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
    type Input = receive::Gated;
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

impl<'d, ID: route::Identity, F: for<'a> Fn(DynamicEvent<'a>) + Unpin> handler::Borrowed<'d, ID>
    for DynamicApp<F>
{
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, DynamicState>,
        chunk: R,
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
            }
            2 => {
                conn.state_mut().phase = 3;
                conn.close_after();
                this.second.hit();
            }
            _ => return Outcome::CloseAfter,
        }
        Outcome::Continue
    }
}
