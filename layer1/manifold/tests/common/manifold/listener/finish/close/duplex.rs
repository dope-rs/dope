use std::{convert, pin::Pin};

use dope_core::{
    driver::{self, route::Identity, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn,
        conn::write,
        handler,
        handler::{Application, failures, retire, send},
    },
};
use dope_net::link::slot::types::CloseGate;
use dope_test::harness::{peer, signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::wire::Wire;

pub const REPLY: &[u8] = b"<<EOF-REPLY>>";

pub struct Fence {
    pub(crate) close: Gate,
}

#[derive(Clone, Copy, Debug)]
pub enum Phase<'d> {
    Fresh,
    Seen(clock::Moment<'d>),
    Held(clock::Moment<'d>),
}

impl<'d, ID: Identity> Application<'d, ID> for Fence {
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
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let Phase::Seen(input) = *conn.state() else {
            panic!("EOF before input");
        };
        *conn.state_mut() = Phase::Held(input);
        let outcome = conn.write().try_bounded(driver, |state, prefix| {
            assert!(matches!(state, Phase::Held(_)));
            prefix[..REPLY.len()].copy_from_slice(REPLY);
            write::Draft::new(REPLY.len(), None, ())
        });
        assert!(matches!(outcome, Ok(write::Outcome::Committed(()))));
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

    fn close<'e>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
    ) {
        assert!(matches!(conn.state(), Phase::Seen(_)));
        self.as_ref().get_ref().close.hit();
    }
}

impl<'d, ID: Identity> handler::Borrowed<'d, ID> for Fence {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        assert!(
            !chunk.as_ref().is_empty() && matches!(conn.state(), Phase::Fresh | Phase::Seen(_))
        );
        *conn.state_mut() = Phase::Seen(driver.driver_ref().clock().now());
        Outcome::Continue
    }
}

impl<'d, ID: Identity> send::Drive<'d, ID> for Fence {
    fn send(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Phase<'d>>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let Phase::Held(input) = *conn.state() else {
            panic!("send before EOF");
        };
        assert!(input <= driver.driver_ref().clock().now());
        *conn.state_mut() = Phase::Seen(input);
    }
}

pub struct Duplex {
    pub(crate) close: Gate,
}

#[derive(Clone, Copy, Debug)]
pub enum Stage {
    Fresh,
    Seen,
}

#[derive(Debug)]
pub enum Flow {
    First(peer::Error),
    Read(peer::Error),
    Second(peer::Error),
    Shutdown(peer::Error),
}

impl<'d, ID: Identity> Application<'d, ID> for Duplex {
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

    fn close<'e>(self: Pin<&mut Self>, conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Wire, Stage>) {
        assert!(matches!(conn.state(), Stage::Seen));
        self.as_ref().get_ref().close.hit();
    }
}

impl<'d, ID: Identity> handler::Borrowed<'d, ID> for Duplex {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, Stage>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        assert!(!chunk.as_ref().is_empty());
        match *conn.state() {
            Stage::Fresh => {
                *conn.state_mut() = Stage::Seen;
                let first = conn.write().try_bounded(driver, |state, prefix| {
                    assert!(matches!(state, Stage::Seen));
                    prefix[..REPLY.len()].copy_from_slice(REPLY);
                    write::Draft::new(REPLY.len(), None, ())
                });
                assert!(matches!(first, Ok(write::Outcome::Committed(()))));
                conn.shutdown();
                let sealed = conn.write().try_bounded(driver, |state, prefix| {
                    assert!(matches!(state, Stage::Seen) && !prefix.is_empty());
                    write::Draft::new(0, None, ())
                });
                assert!(matches!(sealed, Ok(write::Outcome::Closed)));
            }
            Stage::Seen => conn.close_after(),
        }
        Outcome::Continue
    }
}
