use std::{convert, pin::Pin};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::wire::Identity;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

pub enum Event<'d> {
    Chunk(usize),
    Write(Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>),
}

pub struct Drain<F> {
    pub(crate) payload: Vec<u8>,
    pub(crate) gate: Gate,
    pub(crate) observe: F,
}

impl<'d, ID: route::Identity, F: for<'a> Fn(Event<'a>)> Application<'d, ID> for Drain<F> {
    type Build = convert::Infallible;
    type Conn = ();
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

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
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(Event<'a>)> handler::Borrowed<'d, ID> for Drain<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len()));
        let payload = &self.as_ref().get_ref().payload;
        let n = payload.len();
        (self.observe)(Event::Write(conn.write().try_bounded(
            driver,
            |&mut (), prefix| {
                prefix[..n].copy_from_slice(payload);
                write::Draft::new(n, None, ()).close_after()
            },
        )));
        Outcome::Continue
    }
}
