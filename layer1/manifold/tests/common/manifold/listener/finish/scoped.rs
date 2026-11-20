use std::{convert, pin::Pin, rc::Rc};

use dope_core::{
    driver::{self, route, schedule::clock},
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
use o3::buffer::{hold::limits, resident};

pub struct App {
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
    pub(crate) seen: Rc<Trace<Event, 16>>,
}

#[derive(Debug)]
pub enum Event {
    Chunk(Vec<u8>),
    Consumed,
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

    fn close<'e>(self: Pin<&mut Self>, _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, bool>) {
        self.get_mut().closed.hit();
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

    fn scoped_chunk<'s, 'e>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Identity, bool>,
        chunk: scoped::Input<'s, 'w, 'd, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) -> Result<Outcome, scoped::Pending<'s>> {
        let this = self.get_mut();
        let head = chunk.head().expect("scoped data");
        this.seen.record(Event::Chunk(head.input().as_slice().to_vec()));
        if !*conn.state() {
            *conn.state_mut() = true;
            return Err(head.defer());
        }
        head.finish().expect("finish replay").settle()?;
        this.seen.record(Event::Consumed);
        Ok(Outcome::CloseAfter)
    }
}
