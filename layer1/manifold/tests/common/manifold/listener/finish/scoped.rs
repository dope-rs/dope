use std::{convert, num, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::receive::{self, Outcome, limits::Budget},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire},
    },
};
use dope_net::wire::{Identity, receive::scoped};
use dope_test::harness::{observe::Trace, signals::Gate};
use handler::send;
use o3::buffer::{
    hold::{limits, scope},
    resident,
    write::consume::scope::Error,
};

pub struct App {
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
    pub(crate) seen: Rc<Trace<Event, 16>>,
}

#[derive(Debug)]
pub enum Event {
    Chunk(Vec<u8>),
    Consumed(Result<(), Failure>),
}

pub struct Retirement;

impl<'d, ID: route::Identity> Application<'d, ID> for App {
    type Conn = bool;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = resident::Shared<'d>;
    type Input = receive::Scoped;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(false)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, bool>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, bool>
    where
        'd: 'w,
    {
        self.get_mut().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity> retire::Policy<'d, App, ID> for Retirement {
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(self, app: Pin<&mut App>, _: conn::Retired<'_, '_, 'd, ID, Identity, bool>) {
        app.get_mut().retired.hit();
    }
}

impl<'w, 'd: 'w, ID: route::Identity> handler::Scoped<'w, 'd, ID> for App {
    fn retention(&self) -> Budget {
        Budget::new(limits::Bound::new(0), 4096)
    }

    fn scoped_chunk(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Identity, bool>,
        chunk: scoped::Input<'_, 'w, 'd, Identity>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.seen.record(Event::Chunk(chunk.as_ref().to_vec()));
        if !*conn.state() {
            *conn.state_mut() = true;
            return Outcome::Continue;
        }
        let (chunk, budget) = chunk.into_parts();
        let outcome = scope::Input::consume(chunk, budget, |input| {
            let Some(len) = num::NonZero::new(input.as_slice().len()) else {
                return (None, Err(Failure::Empty));
            };
            match input.try_prefix::<{ u32::MAX }>(len) {
                Ok((_, end)) => (Some(end), Ok(())),
                Err(error) => (None, Err(Failure::Prefix(error))),
            }
        });
        this.seen.record(Event::Consumed(outcome));
        Outcome::CloseAfter
    }
}

#[derive(Debug)]
pub enum Failure {
    Empty,
    Prefix(Error),
}
