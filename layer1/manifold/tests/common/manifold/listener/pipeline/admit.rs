use std::{cell::Cell, convert, pin::Pin, rc::Rc};

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
use dope_net::{
    link::egress::{self, load::Value},
    wire::Identity,
};
use dope_test::harness::observe::VecCellExt as _;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::pipeline::prefix::Error;

pub const FIRST_HEADER: &[u8] = b"first:";
pub const FIRST_BODY: &[u8; 5] = b"body;";
pub const SECOND_HEADER: &[u8] = b"second:";
pub const SECOND_BODY: &[u8; 6] = b"owner;";
pub const THIRD_HEADER: &[u8] = b"third:";
pub const THIRD_BODY: &[u8; 6] = b"lease;";

pub const FIRST_REPLY: [u8; 8000] = [0xA1; 8000];
pub const SECOND_REPLY: [u8; 9000] = [0xB2; 9000];

pub enum Event {
    Chunk(usize),
    Write(Result<write::Outcome<()>, Error>, egress::Pending),
}

pub struct Admit {
    pub(crate) gate: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
}

impl<'d, ID: route::Identity> Application<'d, ID> for Admit {
    type Conn = ();
    type Build = convert::Infallible;
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
        self.get_mut().gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Admit {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let event = Event::Chunk(chunk.as_ref().len());
        self.events.push(event);
        for (header, body) in [
            (
                FIRST_HEADER,
                Some(Value::Checked(egress::NonEmpty::from_array(FIRST_BODY))),
            ),
            (
                SECOND_HEADER,
                Some(Value::Checked(egress::NonEmpty::from_array(SECOND_BODY))),
            ),
            (
                THIRD_HEADER,
                Some(Value::Checked(egress::NonEmpty::from_array(THIRD_BODY))),
            ),
            (FIRST_REPLY.as_slice(), None),
            (SECOND_REPLY.as_slice(), None),
        ] {
            let result = conn.write().try_bounded(driver, |&mut (), prefix| {
                prefix[..header.len()].copy_from_slice(header);
                write::Draft::new(header.len(), body, ())
            });
            let event = Event::Write(result.map_err(Error::from), conn.write().pending());
            self.events.push(event);
        }
        Outcome::CloseAfter
    }
}

pub struct Bounded {
    pub(crate) called: Rc<Cell<bool>>,
    pub(crate) gate: Gate,
    pub(crate) attempts: Rc<Cell<Vec<Attempt>>>,
}

pub struct Attempt {
    pub input: usize,
    pub state: u8,
    pub outcome: BoundedWrite,
}

pub enum BoundedWrite {
    Draft {
        cause: error::Bounded,
        written: usize,
        body: Option<Vec<u8>>,
        end: write::End,
    },
    Other(Result<write::Outcome<()>, Error>),
}

impl<'d, ID: route::Identity> Application<'d, ID> for Bounded {
    type Conn = u8;
    type Build = convert::Infallible;
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
        Ok(7)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, u8>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, u8>
    where
        'd: 'w,
    {
        self.get_mut().gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Bounded {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, u8>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let called = &self.as_ref().get_ref().called;
        let result = conn.write().try_bounded(driver, move |state, prefix| {
            called.set(true);
            *state = 9;
            prefix[0] = b'X';
            let body = egress::Load::new(resident::Shared::from(FIRST_BODY.as_slice()));
            write::Draft::new(1, Some(Value::Unchecked(body)), ())
        });
        let outcome = match result {
            Err(error::Error::Mismatch(error::Mismatch::Bounded { cause, draft })) => {
                let (written, body, (), end) = draft.into_parts();
                BoundedWrite::Draft {
                    cause,
                    written,
                    body: body.map(|body| body.as_inner().as_ref().to_vec()),
                    end,
                }
            }
            other => BoundedWrite::Other(other.map_err(Error::from)),
        };
        let attempt = Attempt {
            input: chunk.as_ref().len(),
            state: *conn.state(),
            outcome,
        };
        self.attempts.push(attempt);
        Outcome::CloseAfter
    }
}
