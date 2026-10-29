use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::receive,
    network::listener::handler::{retire, send},
};
use o3::buffer::resident::Shared;

pub mod apps;

use std::{convert, pin::Pin, rc::Rc};

use dope_manifold::{
    app::receive::{Failure, Outcome},
    network::listener::{conn, handler},
};
use dope_net::{
    link::egress::{self, data::compose, load},
    wire::Identity,
};
use dope_test::{
    alloc::Tracker,
    harness::{
        observe::Trace,
        signals::{Gate, Progress as _},
    },
};
use handler::failures;
use o3::buffer::bytes::TryRetain;

use crate::common::manifold::listener::pipeline::prefix::Admission;

pub const RESPONSE: &[u8] = b"associated-egress";

pub struct Release(pub(crate) Gate);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.hit();
    }
}

pub struct App {
    pub(crate) closed: Gate,
    pub(crate) released: Gate,
    pub(crate) returned: Gate,
    pub(crate) retired: Gate,
    pub(crate) abort: bool,
    pub(crate) events: Rc<Trace<Event, 16>>,
}

pub enum Event {
    Chunk(usize, Vec<u8>),
    Invalid(load::Error),
    Admitted(Admission, (usize, usize), usize, usize),
    Released(usize, Vec<u8>, u32, handler::Disposition),
    Retired(usize, u32, u32),
}

pub struct Retirement;

impl<'d, ID: route::Identity> handler::Application<'d, ID> for App {
    type Conn = ();
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = compose::Tagged<Shared<'d>, Release>;
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
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, (), Self::Egress>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, (), Self::Egress>
    where
        'd: 'w,
    {
        self.get_mut().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity> send::Report<'d, ID> for App {
    const ENABLED: bool = true;

    fn released(self: Pin<&mut Self>, release: handler::Release<'d, ID, Self::Egress>) {
        let (conn, egress, cause) = release.into_parts();
        let (body, owner) = egress.into_parts();
        let this = self.get_mut();
        this.events.record(Event::Released(
            conn.index().into_usize(),
            body.as_ref().to_vec(),
            owner.0.hits(),
            cause,
        ));
        this.returned.hit();
    }
}

impl<'d, ID: route::Identity> retire::Policy<'d, App, ID> for Retirement {
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut App>,
        conn: conn::Retired<'_, '_, 'd, ID, Identity, (), compose::Tagged<Shared<'d>, Release>>,
    ) {
        let this = app.get_mut();
        this.events.record(Event::Retired(
            conn.id().index().into_usize(),
            this.released.hits(),
            this.returned.hits(),
        ));
        this.retired.hit();
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for App {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, (), Self::Egress>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        self.events.record(Event::Chunk(
            conn.id().index().into_usize(),
            chunk.as_ref().to_vec(),
        ));
        let released = self.as_ref().get_ref().released.clone();
        let abort = self.as_ref().get_ref().abort;
        if !abort {
            conn.close_after();
        }
        let payload = compose::Tagged::new(Shared::from(RESPONSE), Release(released));
        let before = conn.work().remaining();
        let payload = match egress::NonEmpty::try_new(payload) {
            Ok(payload) => payload,
            Err(error) => {
                self.events.record(Event::Invalid(error));
                return Outcome::Failed(Failure::Contract);
            }
        };
        let (admitted, alloc) = Tracker::usage(|| conn.write().admit(payload, driver));
        self.events.record(Event::Admitted(
            admitted.into(),
            alloc,
            before,
            conn.work().remaining(),
        ));
        if abort {
            Outcome::Failed(Failure::Contract)
        } else {
            Outcome::Continue
        }
    }
}
