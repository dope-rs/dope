use std::{convert, io, pin::Pin};

use dope_core::{
    driver::{retained, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn,
        conn::write::{self, error},
        handler,
        handler::{Application, failures, retire, send},
    },
};
use dope_net::link::slot::types::CloseGate;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::wire::Wire;

pub const REPLY: &[u8] = b"<<EOF-REPLY>>";

pub enum Event<'d> {
    Eof(Phase<'d>),
    Prefix(Phase<'d>),
    Write(Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>),
    Close(Phase<'d>),
    Chunk(usize, Phase<'d>),
    Sent(Phase<'d>, clock::Moment<'d>),
}

pub struct Fence<F> {
    pub(crate) close: Gate,
    pub(crate) observe: F,
}

#[derive(Clone, Copy, Debug)]
pub enum Phase<'d> {
    Fresh,
    Seen(clock::Moment<'d>),
    Held(clock::Moment<'d>),
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> Application<'d, ID> for Fence<F> {
    type Conn = Phase<'d>;
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Run;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(Phase::Fresh)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn eof(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
        _: receive::End,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Eof {
        let phase = *conn.state();
        (self.observe)(Event::Eof(phase));
        if let Phase::Seen(input) = phase {
            *conn.state_mut() = Phase::Held(input);
        }
        let outcome = conn.write().try_bounded(driver, |state, prefix| {
            (self.observe)(Event::Prefix(*state));
            prefix[..REPLY.len()].copy_from_slice(REPLY);
            write::Draft::new(REPLY.len(), None, ())
        });
        (self.observe)(Event::Write(outcome));
        handler::Eof::Hold
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
    ) -> CloseGate {
        match conn.state() {
            Phase::Held(_) => CloseGate::Deferred,
            Phase::Fresh | Phase::Seen(_) => CloseGate::Immediate,
        }
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, Phase<'d>>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, Phase<'d>>
    where
        'd: 'w,
    {
        (self.observe)(Event::Close(*conn.state()));
        self.as_ref().get_ref().close.hit();
        conn
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> handler::Borrowed<'d, ID> for Fence<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(Event::Chunk(chunk.as_ref().len(), *conn.state()));
        *conn.state_mut() = Phase::Seen(driver.driver_ref().clock().now());
        Outcome::Continue
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Event<'a>)> send::Drive<'d, ID> for Fence<F> {
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let phase = *conn.state();
        (self.observe)(Event::Sent(phase, driver.driver_ref().clock().now()));
        if let Phase::Held(input) = phase {
            *conn.state_mut() = Phase::Seen(input);
        }
    }
}

pub enum Half<'d> {
    Close(Stage),
    Chunk(usize),
    Prefix(Stage),
    Write(Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>),
    SealedPrefix(Stage, usize),
    Sealed(Result<write::Outcome<()>, error::Error<resident::Shared<'d>, (), convert::Infallible>>),
}

pub struct Duplex<F> {
    pub(crate) close: Gate,
    pub(crate) observe: F,
}

#[derive(Clone, Copy, Debug)]
pub enum Stage {
    Fresh,
    Seen,
}

#[derive(Debug)]
pub enum Flow {
    First(io::Error),
    Read(io::Error),
    Second(io::Error),
    Shutdown(io::Error),
}

impl<'d, ID: Identity, F: for<'a> Fn(Half<'a>)> Application<'d, ID> for Duplex<F> {
    type Conn = Stage;
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
        Ok(Stage::Fresh)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, Stage>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Wire, Stage>
    where
        'd: 'w,
    {
        (self.observe)(Half::Close(*conn.state()));
        self.as_ref().get_ref().close.hit();
        conn
    }
}

impl<'d, ID: Identity, F: for<'a> Fn(Half<'a>)> handler::Borrowed<'d, ID> for Duplex<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Stage>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        (self.observe)(Half::Chunk(chunk.as_ref().len()));
        match *conn.state() {
            Stage::Fresh => {
                *conn.state_mut() = Stage::Seen;
                let first = conn.write().try_bounded(driver, |state, prefix| {
                    (self.observe)(Half::Prefix(*state));
                    prefix[..REPLY.len()].copy_from_slice(REPLY);
                    write::Draft::new(REPLY.len(), None, ())
                });
                (self.observe)(Half::Write(first));
                conn.shutdown();
                let sealed = conn.write().try_bounded(driver, |state, prefix| {
                    (self.observe)(Half::SealedPrefix(*state, prefix.len()));
                    write::Draft::new(0, None, ())
                });
                (self.observe)(Half::Sealed(sealed));
            }
            Stage::Seen => conn.close_after(),
        }
        Outcome::Continue
    }
}
