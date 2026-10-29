use std::{convert, pin::Pin, time};

use dope_core::{
    driver::{retained, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, deadline::Arm},
        handler::{self, failures, retire, send},
    },
};
use dope_net::{link::slot::types::CloseGate, wire::Identity};
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

pub struct Lifetime<F> {
    pub(crate) accept: Gate,
    pub(crate) expire: Gate,
    pub(crate) begun: Gate,
    pub(crate) closed: Gate,
    pub(crate) expiry: Expiry,
    pub(crate) drain: handler::Drain,
    pub(crate) gate: CloseGate,
    pub(crate) deadline: F,
}

#[derive(Clone, Copy)]
pub enum Expiry {
    Discard,
    Clear,
}

impl<'d, ID: route::Identity, F: for<'a> Fn(Arm<'a>)> handler::Application<'d, ID> for Lifetime<F> {
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

    fn timing(&self) -> handler::Timing {
        match self.expiry {
            Expiry::Clear => handler::Timing::timeout(time::Duration::from_millis(20)),
            Expiry::Discard => handler::Timing::default(),
        }
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timeout(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        let this = self.as_ref().get_ref();
        this.expire.hit();
        match this.expiry {
            Expiry::Discard => handler::Timeout::Discard,
            Expiry::Clear => handler::Timeout::Continue(handler::Deadline::Clear),
        }
    }

    fn drain(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        let this = self.as_ref().get_ref();
        this.begun.hit();
        this.drain
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
    ) -> Outcome {
        let this = self.as_ref().get_ref();
        if matches!(this.expiry, Expiry::Clear) {
            conn.wake_target().wake();
        } else {
            this.accept.hit();
        }
        Outcome::Continue
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        let this = self.as_ref().get_ref();
        if matches!(this.expiry, Expiry::Clear) {
            let now = driver.driver_ref().clock().now();
            (this.deadline)(conn.deadline().try_ensure(now));
            let at = now.clamp_add(time::Duration::from_millis(10));
            for _ in 0..2 {
                (this.deadline)(conn.deadline().try_arm(at));
            }
            this.accept.hit();
        }
        conn
    }

    fn close_gate(
        self: Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Identity, ()>,
    ) -> CloseGate {
        self.gate
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity, F: for<'a> Fn(Arm<'a>)> handler::Borrowed<'d, ID> for Lifetime<F> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        _: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        Outcome::Continue
    }
}
