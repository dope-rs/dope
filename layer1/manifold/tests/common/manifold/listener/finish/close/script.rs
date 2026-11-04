use std::{convert, net, pin::Pin, time};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_test::harness::{peer, peer::Peer, signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::wire::Wire;

pub struct App {
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) close: Gate,
    pub(crate) chunk: Option<Gate>,
}

#[derive(Debug)]
pub enum Read {
    Connect(peer::Error),
    Write(peer::Error),
    Receive(peer::Error),
}

pub struct Script(pub(crate) net::SocketAddr);
impl Script {
    pub fn try_read(self) -> Result<Vec<u8>, Read> {
        let stream = Peer::at(self.0)
            .try_read(time::Duration::from_secs(10))
            .map_err(Read::Connect)?;
        stream.write_all(b"REQ").map_err(Read::Write)?;
        Peer::try_read_all(&stream).map_err(Read::Receive)
    }
}

impl<'d, ID: Identity> Application<'d, ID> for App {
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
        assert!(*conn.state() <= 1);
        self.as_ref().get_ref().close.hit();
        conn
    }
}

impl<'d, ID: Identity> handler::Borrowed<'d, ID> for App {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, u8>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        assert!(!chunk.as_ref().is_empty());
        if let Some(gate) = self.as_ref().get_ref().chunk.as_ref() {
            gate.hit();
        }
        let Some(reply) = self.as_ref().get_ref().payload.as_ref() else {
            return Outcome::Continue;
        };
        let n = reply.len();
        let outcome = conn.write().try_bounded(driver, |state, prefix| {
            assert_eq!(*state, 0);
            *state = 1;
            prefix[..n].copy_from_slice(reply);
            write::Draft::new(n, None, ())
        });
        assert!(matches!(outcome, Ok(write::Outcome::Committed(()))));
        Outcome::CloseAfter
    }
}
