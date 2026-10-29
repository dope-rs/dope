use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::egress::{self, load::Value},
    wire::Identity,
};
use dope_test::harness::observe::VecCellExt as _;
use dope_test::{alloc::Tracker, harness::signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::pipeline::prefix::Error;

pub enum Event {
    Chunk(usize),
    Write(Option<(usize, u8)>, Result<write::Outcome<()>, Error>),
}

pub struct Wake {
    pub(crate) gate: Gate,
    pub(crate) commits: Rc<Cell<u32>>,
    pub(crate) defers: Rc<Cell<u32>>,
    pub(crate) wakes: Rc<Cell<u32>>,
    pub(crate) changed: Cell<bool>,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
}

impl Wake {
    pub(crate) fn drain<'d, ID: route::Identity>(
        &self,
        conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, u8>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        while *conn.state() < 3 {
            let commits = self.commits.clone();
            let mut callback = None;
            let result = if self.changed.replace(false) {
                let body = egress::NonEmpty::from_array(b"C");
                let body = Value::Checked(body);
                conn.write().try_bounded(driver, |state, prefix| {
                    callback = Some((prefix.len(), *state));
                    *state += 1;
                    commits.set(commits.get() + 1);
                    write::Draft::new(0, Some(body), ()).close_after()
                })
            } else {
                conn.write().try_bounded(driver, move |state, prefix| {
                    prefix[0] = b'A' + *state;
                    *state += 1;
                    commits.set(commits.get() + 1);
                    write::Draft::new(1, None, ())
                })
            };
            let ready = matches!(&result, Ok(write::Outcome::Committed(())));
            if matches!(&result, Ok(write::Outcome::Deferred(_))) {
                self.defers.set(self.defers.get() + 1);
                self.changed.set(true);
            } else if !ready {
                conn.close_after();
            }
            let event = Event::Write(callback, result.map_err(Error::from));
            self.events.push(event);
            if !ready {
                return;
            }
        }
    }
}

impl<'d, ID: route::Identity> Application<'d, ID> for Wake {
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
        Ok(0)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, u8>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, u8>
    where
        'd: 'w,
    {
        let this = self.get_mut();
        this.wakes.set(this.wakes.get() + 1);
        this.drain(&mut conn, driver);
        conn
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

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Wake {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, u8>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let event = Event::Chunk(chunk.as_ref().len());
        self.events.push(event);
        self.get_mut().drain(&mut conn, driver);
        Outcome::Continue
    }
}

pub struct Zero {
    pub(crate) gate: Gate,
    pub(crate) attempts: Rc<Cell<Vec<ZeroEvent>>>,
}

pub enum ZeroEvent {
    Zero {
        input: usize,
        before: usize,
        capacity: Option<usize>,
        result: Result<write::Outcome<u8>, Error>,
        allocations: (usize, usize),
        remaining: usize,
        pending: egress::Pending,
    },
    Sent {
        result: Result<write::Outcome<()>, Error>,
        allocations: (usize, usize),
        remaining: usize,
    },
}

impl<'d, ID: route::Identity> Application<'d, ID> for Zero {
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

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Zero {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let before = conn.work().remaining();
        let mut capacity = None;
        let measure = Tracker::begin();
        let zero = conn.write().try_bounded(driver, |&mut (), prefix| {
            capacity = Some(prefix.len());
            write::Draft::new(0, None, 7)
        });
        let activity = measure.finish();
        let zero = ZeroEvent::Zero {
            input: chunk.as_ref().len(),
            before,
            capacity,
            result: zero.map_err(Error::from),
            allocations: (activity.allocations, activity.allocated_bytes),
            remaining: conn.work().remaining(),
            pending: conn.write().pending(),
        };
        let measure = Tracker::begin();
        let sent = conn.write().try_bounded(driver, |&mut (), prefix| {
            prefix[0] = b'Z';
            write::Draft::new(1, None, ()).close_after()
        });
        let activity = measure.finish();
        let sent = ZeroEvent::Sent {
            result: sent.map_err(Error::from),
            allocations: (activity.allocations, activity.allocated_bytes),
            remaining: conn.work().remaining(),
        };
        let mut attempts = self.attempts.take();
        attempts.extend([zero, sent]);
        self.attempts.set(attempts);
        Outcome::Continue
    }
}
