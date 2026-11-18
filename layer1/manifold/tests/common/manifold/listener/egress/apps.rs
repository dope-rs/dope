use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use conn::write;
use dope_core::{
    driver::{self, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{conn, handler, handler::send},
};
use dope_net::{
    link::egress::{self, data},
    wire::Identity,
};
use dope_test::{alloc::Tracker, harness::signals::Gate};
use handler::{failures, retire};
use o3::buffer::{
    bytes::TryRetain,
    resident::{Shared, bound},
    storage,
};
use write::error;

use crate::common::manifold::listener::egress::{RESPONSE, Retirement};

pub struct State<'d> {
    pub(crate) body: Option<Shared<'d>>,
}

pub struct Direct<F> {
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
    pub(crate) body: Cell<Option<storage::Shared>>,
    pub(crate) observe: F,
}

pub struct DirectWrite {
    pub body: Option<usize>,
    pub allocations: (usize, usize),
    pub result: Result<(), error::Admission>,
}

impl<'d, ID: route::Identity, F: Fn(DirectWrite)> handler::Application<'d, ID> for Direct<F> {
    type Conn = State<'d>;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = data::Bound<Shared<'d>, bound::Unbounded>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State {
            body: self.body.take().map(Shared::from),
        })
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
    ) {
        self.as_ref().get_ref().closed.hit();
    }
}

impl<'d, ID: route::Identity, F: Fn(DirectWrite)> handler::Borrowed<'d, ID> for Direct<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
        _: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        let mut consumed = None;
        let measure = Tracker::begin();
        {
            let mut place = std::mem::MaybeUninit::uninit();
            let mut abandoned = conn
                .try_output(driver, &mut place)
                .expect("fixed output");
            abandoned
                .get_mut()
                .parts()
                .1
                .write_byte(0xff)
                .expect("prefix capacity");
            std::mem::forget(abandoned);
        }
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let result = match conn.try_output(driver, &mut output_slot) {
            Ok(mut output) => {
                let (state, mut prefix) = output.get_mut().parts();
                {
                    let mut staged = prefix.try_split(1, 2).expect("staged capacity");
                    staged.body().write_slice(b"xx").expect("staged body");
                }
                prefix.write_byte(0xa5).expect("prefix capacity");
                let body = state.body.take();
                consumed = body.as_ref().map(|body| body.as_ref().len());
                let body = body.map(|body| {
                    egress::load::Value::Unchecked(egress::Load::new(body).unbounded())
                });
                match body {
                    Some(body) => write::Output::try_publish(output, body, write::End::Close),
                    None => write::Output::finish(output, write::End::Close),
                }
                .map_err(error::Mismatch::into_cause)
                .expect("publication");
                Ok(())
            }
            Err(cause) => Err(cause),
        };
        let activity = measure.finish();
        let allocs = (activity.allocations, activity.allocated_bytes);
        (self.observe)(DirectWrite {
            body: consumed,
            allocations: allocs,
            result,
        });
        Outcome::Continue
    }
}

pub struct Bounded<F> {
    pub(crate) closed: Gate,
    pub(crate) retired: Gate,
    pub(crate) called: Rc<Cell<bool>>,
    pub(crate) observe: F,
}

pub enum BoundedEvent {
    Resident(Option<egress::ResidentSize>),
    Write {
        body: Option<usize>,
        allocations: (usize, usize),
        result: Result<(), error::Admission>,
    },
    Deferred {
        body: Option<Option<usize>>,
        called: bool,
        result: Result<(), error::Admission>,
    },
    Activated {
        body: Option<Option<usize>>,
        result: Result<(), error::Admission>,
    },
}

impl<'d, ID: route::Identity, F: Fn(BoundedEvent)> handler::Application<'d, ID> for Bounded<F> {
    type Conn = State<'d>;
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = Retirement;
    type Egress = data::Bound<Shared<'d>, bound::AtMost<0>>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(State {
            body: Some(Shared::from(RESPONSE)),
        })
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn activate<'e>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.as_ref().get_ref();
        let called = &this.called;
        if called.get() {
            return;
        }
        let mut body = None;
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let result = match conn.try_output(driver, &mut output_slot) {
            Ok(mut output) => {
                let (state, mut prefix) = output.get_mut().parts();
                called.set(true);
                body = Some(state.body.as_ref().map(|body| body.as_ref().len()));
                prefix.write_byte(0xc5).expect("prefix capacity");
                write::Output::finish(output, write::End::Close)
                    .map_err(error::Mismatch::into_cause)
                    .expect("publication");
                Ok(())
            }
            Err(cause) => Err(cause),
        };
        (this.observe)(BoundedEvent::Activated { body, result });
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
    ) {
        self.as_ref().get_ref().closed.hit();
    }
}

impl<'d, ID: route::Identity, F: Fn(BoundedEvent)> handler::Borrowed<'d, ID> for Bounded<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, State<'d>, Self::Egress>,
        _: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        (self.observe)(BoundedEvent::Resident(
            conn.state()
                .body
                .as_ref()
                .map(|body| egress::Charge::of(body).resident()),
        ));
        let mut consumed = None;
        let measure = Tracker::begin();
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let result = match conn.try_output(driver, &mut output_slot) {
            Ok(mut output) => {
                let (state, mut prefix) = output.get_mut().parts();
                prefix.write_byte(0xb5).expect("prefix capacity");
                let body = state.body.take();
                consumed = body.as_ref().map(|body| body.as_ref().len());
                let body = body.map(|body| {
                    egress::Load::new(body)
                        .try_bound::<bound::AtMost<0>>()
                        .map_err(|excess| excess.maximum)
                        .expect("static fixture retention")
                });
                match body {
                    Some(body) => write::Output::try_publish(
                        output,
                        egress::load::Value::Unchecked(body),
                        write::End::Open,
                    ),
                    None => write::Output::finish(output, write::End::Open),
                }
                .map_err(error::Mismatch::into_cause)
                .expect("publication");
                Ok(())
            }
            Err(cause) => Err(cause),
        };
        let activity = measure.finish();
        let allocs = (activity.allocations, activity.allocated_bytes);
        (self.observe)(BoundedEvent::Write {
            body: consumed,
            allocations: allocs,
            result,
        });
        let called = self.as_ref().get_ref().called.clone();
        let mut body = None;
        let mut output_slot = std::mem::MaybeUninit::uninit();
        let result = match conn.try_output(driver, &mut output_slot) {
            Ok(mut output) => {
                let (state, _) = output.get_mut().parts();
                called.set(true);
                body = Some(state.body.take().map(|body| body.as_ref().len()));
                write::Output::finish(output, write::End::Open)
                    .map_err(error::Mismatch::into_cause)
                    .expect("empty publication");
                Ok(())
            }
            Err(cause) => Err(cause),
        };
        (self.observe)(BoundedEvent::Deferred {
            body,
            called: self.as_ref().get_ref().called.get(),
            result,
        });
        Outcome::Continue
    }
}

impl<'d, ID: route::Identity, F: Fn(DirectWrite)> retire::Policy<'d, Direct<F>, ID> for Retirement {
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Direct<F>>,
        _: conn::Retired<
            '_,
            '_,
            'd,
            ID,
            Identity,
            State<'d>,
            data::Bound<Shared<'d>, bound::Unbounded>,
        >,
    ) {
        app.as_ref().get_ref().retired.hit();
    }
}

impl<'d, ID: route::Identity, F: Fn(BoundedEvent)> retire::Policy<'d, Bounded<F>, ID>
    for Retirement
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: Pin<&mut Bounded<F>>,
        _: conn::Retired<
            '_,
            '_,
            'd,
            ID,
            Identity,
            State<'d>,
            data::Bound<Shared<'d>, bound::AtMost<0>>,
        >,
    ) {
        app.as_ref().get_ref().retired.hit();
    }
}

use o3::buffer::write::Sink as _;
