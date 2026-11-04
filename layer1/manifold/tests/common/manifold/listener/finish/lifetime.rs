use std::{cell::Cell, convert, num::NonZero, pin::Pin, rc::Rc, time};

use dope_core::{
    driver::{
        retained, route,
        schedule::{clock, ready},
    },
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, deadline::Arm},
        handler::{self, failures, retire, send},
    },
};
use dope_net::{
    link::slot::types::CloseGate,
    wire::{
        Identity,
        receive::{Chunk, mode::Disabled},
    },
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::buffer::{bytes::TryRetain, resident};

pub struct Lifetime {
    pub(crate) accept: Gate,
    pub(crate) expire: Gate,
    pub(crate) begun: Gate,
    pub(crate) closed: Gate,
    pub(crate) expiry: Expiry,
    pub(crate) drain: handler::Drain,
    pub(crate) gate: CloseGate,
}

fn deadline(outcome: Arm<'_>) {
    assert!(matches!(outcome, Arm::Armed));
}

pub enum Expiry {
    Discard,
    Clear,
    Rearm {
        slot: usize,
        count: Option<NonZero<u32>>,
        events: Rc<Trace<Event, 4>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Drain(usize),
    Close(usize),
}

pub struct Connection<'d> {
    deadline: Option<clock::Deadline<'d>>,
    remaining: Option<NonZero<u32>>,
}

impl<'d, ID: route::Identity> handler::Application<'d, ID> for Lifetime {
    type Conn = Connection<'d>;
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
        Ok(Connection {
            deadline: None,
            remaining: match &self.expiry {
                Expiry::Rearm { count, .. } => *count,
                Expiry::Discard | Expiry::Clear => None,
            },
        })
    }

    fn timing(&self) -> handler::Timing {
        match &self.expiry {
            Expiry::Clear => handler::Timing::timeout(time::Duration::from_millis(20)),
            Expiry::Discard => handler::Timing::default(),
            Expiry::Rearm { .. } => handler::Timing::managed(),
        }
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn timeout(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        let this = self.as_ref().get_ref();
        this.expire.hit();
        match &this.expiry {
            Expiry::Discard => handler::Timeout::Discard,
            Expiry::Clear => handler::Timeout::Continue(handler::Deadline::Clear),
            Expiry::Rearm { .. } => {
                let state = conn.state_mut();
                if let Some(remaining) = state.remaining {
                    state.remaining = NonZero::new(remaining.get() - 1);
                    if state.remaining.is_none() {
                        return handler::Timeout::Continue(handler::Deadline::Clear);
                    }
                }
                handler::Timeout::Continue(handler::Deadline::Arm(
                    state.deadline.expect("initial application deadline"),
                ))
            }
        }
    }

    fn drain(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        let this = self.as_ref().get_ref();
        if let Expiry::Rearm { events, .. } = &this.expiry {
            events.record(Event::Drain(conn.id().index().into_usize()));
        }
        this.begun.hit();
        this.drain
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
    ) -> Outcome {
        let this = self.as_ref().get_ref();
        if matches!(this.expiry, Expiry::Clear | Expiry::Rearm { .. }) {
            conn.wake_target().wake();
        } else {
            this.accept.hit();
        }
        Outcome::Continue
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn>
    where
        'd: 'w,
    {
        let this = self.as_ref().get_ref();
        if matches!(this.expiry, Expiry::Clear) {
            let now = driver.driver_ref().clock().now();
            deadline(conn.deadline().try_ensure(now));
            let at = now.clamp_add(time::Duration::from_millis(10));
            for _ in 0..2 {
                deadline(conn.deadline().try_arm(at));
            }
            this.accept.hit();
        } else if let Expiry::Rearm { slot, .. } = &this.expiry
            && conn.state().deadline.is_none()
        {
            let at = driver.driver_ref().clock().now().into();
            conn.state_mut().deadline = Some(at);
            if conn.id().index().into_usize() == *slot {
                deadline(conn.deadline().try_arm(at));
            }
            this.accept.hit();
        }
        conn
    }

    fn close_gate(
        self: Pin<&Self>,
        _: conn::Ref<'_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
    ) -> CloseGate {
        self.gate
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, Self::Conn>
    where
        'd: 'w,
    {
        let this = self.as_ref().get_ref();
        if let Expiry::Rearm { events, .. } = &this.expiry {
            events.record(Event::Close(conn.id().index().into_usize()));
        }
        this.closed.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Lifetime {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, Self::Conn>,
        _: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        Outcome::Continue
    }
}

pub struct DeadlineAdmission<'d> {
    pub(crate) target: Rc<Cell<Option<ready::Target<'d>>>>,
    pub(crate) activate: Rc<Cell<bool>>,
    pub(crate) completed: Gate,
    pub(crate) expired: Gate,
}

impl<'d, ID: route::Identity> handler::Application<'d, ID> for DeadlineAdmission<'d> {
    type Conn = ();
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Owned;
    type Send = send::Silent;

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<(), Self::Build> {
        Ok(())
    }

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn timing(&self) -> handler::Timing {
        handler::Timing::timeout(time::Duration::from_millis(10))
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
    ) -> Outcome {
        let now = conn.work().clock().now();
        assert!(matches!(conn.deadline().try_ensure(now), Arm::Armed));
        self.target.set(Some(conn.wake_target()));
        Outcome::Continue
    }

    fn timeout(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Timeout<'d> {
        self.expired.hit();
        self.completed.hit();
        handler::Timeout::Discard
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        if self.activate.get() {
            conn.deadline().cancel();
            self.completed.hit();
        }
        conn
    }
}

impl<'w, 'd: 'w, ID: route::Identity> handler::Owned<'w, 'd, ID> for DeadlineAdmission<'d> {
    const RETENTION: receive::limits::Retention = receive::limits::Retention::new(1, 0);

    fn chunk(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Identity, ()>,
        _: Chunk<'w, 'd, Identity, Disabled>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        conn.deadline().cancel();
        self.completed.hit();
        Outcome::Continue
    }
}
