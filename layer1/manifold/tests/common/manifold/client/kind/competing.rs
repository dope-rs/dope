use std::{cell::Cell, rc::Rc};

use dope_core::{
    driver::{self, route::KeyTag, schedule},
    io::socket,
};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt, close, conn,
    session::{
        self, Output, Parser, Plain, Resume, Retirement, Scheduling, Session, Target,
        app::{drain::Ready, request::Source},
        codec::mode,
        policy,
        response::{self, work},
        turns,
    },
};
use dope_net::{
    link::{egress, pool::change::opening},
    wire::{self, Identity},
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{
    base::cell::region,
    buffer::{self, storage::Shared, write::consume::scope},
};

use crate::common::manifold::client::switch::terminal::plaintext::{Decode, Event};

pub struct Competing {
    pub(crate) left: Rc<Cell<usize>>,
    pub(crate) at_resume: Rc<Cell<usize>>,
    pub(crate) resumed: Gate,
    pub(crate) drained: Gate,
    pub(crate) errors: Rc<Trace<Event, 8>>,
}

struct Pending<'a>(&'a Cell<usize>);

impl<'d> schedule::app::Drain<region::Token<region::Identity<'d>>> for Pending<'_> {
    type Candidate<'a>
        = ()
    where
        Self: 'a;
    type Item<'a>
        = ()
    where
        Self: 'a;

    fn next_when<P>(
        &mut self,
        _: &mut region::Token<region::Identity<'d>>,
        take: impl FnOnce() -> Option<P>,
    ) -> schedule::app::Selection<(), P, ()> {
        let left = self.0.get().checked_sub(1)?;
        Some(match take() {
            Some(permit) => {
                self.0.set(left);
                Ok(((), permit))
            }
            None => Err(()),
        })
    }
}

impl<'d> Session<'d> for Competing {
    type Codec = Decode;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Receipt = ();
    type Requests = request::Enabled;
    type Send = Shared;

    fn codec(&self) -> &Decode {
        &Decode
    }
}

impl<'d> Parser<'d> for Competing {
    type Mode = mode::Plain;
    type Response = response::Resume<(), ()>;
}

impl<'d> Plain<'d> for Competing {
    type Delivery = response::Delivery<()>;

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        _: u8,
        _: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> Self::Delivery
    where
        'd: 'input,
    {
        response::Delivery::Prepared(())
    }

    fn reject(
        &mut self,
        cause: scope::Error,
        ctx: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> Self::Delivery {
        self.errors.record(Event::Decode(cause));
        ctx.request(close::Directive::cut_final(close::Reason::Protocol));
        response::Delivery::Committed
    }
}

impl<'d> Resume<'d, (), ()> for Competing {
    fn commit<A: work::Admission>(
        &mut self,
        _: response::Committed<()>,
        _: &mut A,
        ctx: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> response::Settlement<()> {
        self.left.set(3 * schedule::LIMIT);
        ctx.wake_target().wake();
        response::Settlement::Yield(())
    }

    fn settle<A: work::Admission>(
        &mut self,
        _: response::Item<()>,
        _: &mut A,
        _: &mut turns::Context<'_, '_, 'd, (), Shared>,
    ) -> response::Settlement<()> {
        self.at_resume.set(self.left.get());
        self.resumed.hit();
        response::Settlement::Ready
    }
}

impl<'d> Source<'d> for Competing {
    fn drain<'w, 'e, 'turn, W: wire::Spec + 'd>(
        &self,
        turn: &mut request::Turn<
            'turn,
            'e,
            'd,
            session::Connection<'w, 'd, KeyTag<0>, Self, W>,
            Shared,
        >,
    ) where
        'd: 'w,
    {
        let before = self.left.get();
        let mut source = Pending(&self.left);
        let (mut requests, _) = turn.requests();
        while let Some(((), work)) = requests.claim(&mut source) {
            drop(work);
        }
        if before != 0 && self.left.get() == 0 {
            self.drained.hit();
        }
    }
}

impl<'d> Output<'d> for Competing {}
impl<'d> Scheduling<'d> for Competing {}
impl<'d> Target<'d, KeyTag<0>, 1> for Competing {}

impl<'d> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Competing {
    type Open = Self;
    type Terminal = Self;
    fn open(&self) -> &Self {
        self
    }
    fn terminal(&self) -> &Self {
        self
    }
}

impl<'d> session::Connected<'d> for Competing {
    type Activation = Automatic;
    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, (), Shared>,
    ) {
    }
}

impl<'d> session::Failed<'d> for Competing {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.errors.record(Event::Connect(cause));
    }
}

impl<'d> session::Exhausted<'d> for Competing {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.errors.record(Event::Exhausted(cause));
    }
}

impl<'d> Retirement<'d> for Competing {
    type Drain = Ready;
    fn io_fail(&mut self, cause: dope_core::Errno, _: &mut turns::Context<'_, '_, 'd, (), Shared>) {
        self.errors.record(Event::Receive(cause));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut (),
        _: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, (), KeyTag<0>, Self>) {}
}
