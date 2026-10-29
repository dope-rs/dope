use std::{convert, io, io::Write as _, net, pin::Pin, time};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_test::harness::{peer, peer::Peer, signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::wire::Wire;

pub enum Event<'d> {
    Close(u8),
    Chunk(usize),
    Prefix(u8),
    Write(Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>),
}

pub struct App<F> {
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) close: Gate,
    pub(crate) chunk: Option<Gate>,
    pub(crate) observe: F,
}

#[derive(Debug)]
pub enum Read {
    Connect(peer::Connect),
    Write(io::Error),
    Receive(io::Error),
}

pub struct Script(pub(crate) net::SocketAddr);
impl Script {
    pub fn try_read(self) -> Result<Vec<u8>, Read> {
        let mut stream = Peer::at(self.0)
            .try_read(time::Duration::from_secs(10))
            .map_err(Read::Connect)?;
        stream.write_all(b"REQ").map_err(Read::Write)?;
        Peer::try_read_all(&mut stream).map_err(Read::Receive)
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> Application<'d, ID> for App<F> {
    type Conn = u8;
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(0)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn drain<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, u8>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        handler::Drain::Complete
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, u8>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, u8>
    where
        'd: 'w,
    {
        (self.observe)(Event::Close(*conn.state()));
        self.as_ref().get_ref().close.hit();
        conn
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> handler::Borrowed<'d, ID> for App<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, u8>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len()));
        if let Some(gate) = self.as_ref().get_ref().chunk.as_ref() {
            gate.hit();
        }
        let Some(reply) = self.as_ref().get_ref().payload.as_ref() else {
            return Outcome::Continue;
        };
        let n = reply.len();
        let outcome = conn.write().try_bounded(driver, |state, prefix| {
            (self.observe)(Event::Prefix(*state));
            *state = 1;
            prefix[..n].copy_from_slice(reply);
            write::Draft::new(n, None, ())
        });
        (self.observe)(Event::Write(outcome));
        Outcome::CloseAfter
    }
}
