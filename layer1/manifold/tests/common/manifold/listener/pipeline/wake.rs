use o3::buffer::write::Sink as _;
use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use dope_core::{
    driver::{self, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{link::egress, wire::Identity};
use dope_test::harness::observe::VecCellExt as _;
use dope_test::{alloc::Tracker, harness::signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

pub enum Event {
    Chunk(usize),
    Write(Option<u8>, Result<(), write::error::Admission>),
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
        driver: &mut driver::Context<'_, 'd>,
    ) {
        while *conn.state() < 3 {
            let commits = self.commits.clone();
            let mut callback = None;
            let result = if self.changed.replace(false) {
                match conn
                    .write()
                    .admit(egress::NonEmpty::from_array(b"C"), driver)
                {
                    write::body::Admit::Committed => {
                        let state = conn.state_mut();
                        callback = Some(*state);
                        *state += 1;
                        commits.set(commits.get() + 1);
                        conn.close_after();
                        Ok(())
                    }
                    write::body::Admit::Deferred { cause, .. } => {
                        Err(write::error::Admission::Deferred(cause))
                    }
                    write::body::Admit::Closed(_) => Err(write::error::Admission::Closed),
                    write::body::Admit::Rejected { cause, .. } => {
                        Err(write::error::Admission::Rejected(cause))
                    }
                    write::body::Admit::Unavailable(cause) => {
                        Err(write::error::Admission::Rejected(write::Reject::Geometry(
                            egress::reserve::error::Full::Reserve(cause.into_parts().1),
                        )))
                    }
                }
            } else {
                let mut output_slot = std::mem::MaybeUninit::uninit();
                match conn.write().try_prefix(driver, &mut output_slot) {
                    Ok(mut output) => {
                        let (state, mut prefix) = output.get_mut().parts();
                        prefix.write_byte(b'A' + *state).unwrap();
                        *state += 1;
                        commits.set(commits.get() + 1);
                        write::Output::finish(output, write::End::Open)
                            .map_err(write::error::Mismatch::into_cause)
                            .expect("prefix publication");
                        Ok(())
                    }
                    Err(cause) => Err(cause),
                }
            };
            let ready = matches!(&result, Ok(()));
            if matches!(&result, Err(write::error::Admission::Deferred(_))) {
                self.defers.set(self.defers.get() + 1);
                self.changed.set(true);
            } else if !ready {
                conn.close_after();
            }
            let event = Event::Write(callback, result);
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

    fn activate<'e>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, u8>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.get_mut();
        this.wakes.set(this.wakes.get() + 1);
        this.drain(&mut conn, driver);
    }

    fn close<'e>(self: Pin<&mut Self>, _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, u8>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Wake {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, u8>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
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
        result: Result<u8, write::error::Admission>,
        allocations: (usize, usize),
        remaining: usize,
        pending: egress::Pending,
    },
    Sent {
        result: Result<(), write::error::Admission>,
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

    fn close<'e>(self: Pin<&mut Self>, _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, ()>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Zero {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        let before = conn.work().remaining();
        let mut capacity = None;
        let measure = Tracker::begin();
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let zero = match conn.write().try_prefix(driver, &mut output_slot) {
            Ok(mut output) => {
                capacity = Some(output.get_mut().parts().1.remaining());
                write::Output::finish(output, write::End::Open)
                    .map_err(write::error::Mismatch::into_cause)
                    .expect("empty publication");
                Ok(7)
            }
            Err(cause) => Err(cause),
        };
        let activity = measure.finish();
        let zero = ZeroEvent::Zero {
            input: chunk.as_ref().len(),
            before,
            capacity,
            result: zero,
            allocations: (activity.allocations, activity.allocated_bytes),
            remaining: conn.work().remaining(),
            pending: conn.write().pending(),
        };
        let measure = Tracker::begin();
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let sent = match conn.write().try_prefix(driver, &mut output_slot) {
            Ok(mut output) => {
                output.get_mut().parts().1.write_byte(b'Z').unwrap();
                write::Output::finish(output, write::End::Close)
                    .map_err(write::error::Mismatch::into_cause)
                    .expect("prefix publication");
                Ok(())
            }
            Err(cause) => Err(cause),
        };
        let activity = measure.finish();
        let sent = ZeroEvent::Sent {
            result: sent,
            allocations: (activity.allocations, activity.allocated_bytes),
            remaining: conn.work().remaining(),
        };
        let mut attempts = self.attempts.take();
        attempts.extend([zero, sent]);
        self.attempts.set(attempts);
        Outcome::Continue
    }
}
