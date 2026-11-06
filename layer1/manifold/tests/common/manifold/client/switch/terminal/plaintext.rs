use std::{num::NonZero, rc::Rc};

use dope_core::{
    driver,
    driver::{route::KeyTag, schedule::clock},
    io::socket,
};
use dope_manifold::client::{
    app::{admit::Automatic, connect::Full, failure::Observed, request},
    attempt, close, conn,
    session::{
        self, Output, Parser, Plain, Resume, Retirement, Scheduling, Session, Target,
        app::drain::Ready,
        codec::{self, Codec, Stateless, eof, mode, staging::Buffered},
        policy::{self, end},
        response,
        response::work,
        turns,
    },
};
use dope_net::{
    link::{egress, pool::change::opening},
    wire::{Identity, failure, terminal},
};
use dope_test::harness::{
    observe::Trace,
    signals::{Gate, Progress as _},
};
use o3::{
    base::cell::region,
    buffer::{self, hold, resident::staging, storage::Shared, write::consume::scope},
};

use super::failure::{Cause, Fault};

pub struct Decode;

impl Codec for Decode {
    type Head<'input, 'd, R: buffer::Cursor>
        = u8
    where
        R: 'input,
        'd: 'input;
    type Error = scope::Error;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(2, 2)
    }
}

impl Stateless for Decode {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Parse<'id, u8>, scope::Error>
    where
        'd: 'input,
    {
        let head = match input.first() {
            Some(&head) if input.as_slice() != b"P" => head,
            _ => {
                return Ok(match input.need_more(None) {
                    Some(need) => codec::Parse::More(need),
                    None => codec::Parse::CapacityExhausted,
                });
            }
        };
        let consumed = input.try_consumed(NonZero::<usize>::MIN)?;
        Ok(codec::Parse::Item { head, consumed })
    }
}

impl eof::Finish for Decode {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        _: codec::Input<'id, 'input, 'd, R>,
    ) -> Result<codec::Finish<u8>, scope::Error>
    where
        'd: 'input,
    {
        Ok(codec::Finish::Empty)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    Ready,
    Yield,
    Blocked,
    Timeout,
}

#[derive(Debug)]
pub enum Event {
    Parse {
        pointer: *const u8,
        len: usize,
    },
    Head(u8),
    Settled {
        byte: u8,
        closing: bool,
        expired: bool,
    },
    Failed(Cause, Option<Cause>),
    Quarantined,
    Closed(close::Reason),
    Decode(scope::Error),
    Connect(attempt::ConnectFault),
    Exhausted(opening::Exhaustion),
    Receive(dope_core::Errno),
}

pub struct Plaintext {
    pub(crate) mode: Mode,
    pub(crate) events: Rc<Trace<Event, 8>>,
    pub(crate) conn: Gate,
    pub(crate) parsed: Option<Gate>,
    pub(crate) closed: Gate,
}

impl<'d> Session<'d> for Plaintext {
    type Codec = Decode;
    type Connect = Full;
    type Requests = request::Disabled;
    type ConnState = Option<clock::Deadline<'d>>;
    type Failure = Observed;
    type Receipt = ();
    type Send = Shared;

    fn codec(&self) -> &Decode {
        &Decode
    }
}

impl<'d> Parser<'d> for Plaintext {
    type Mode = mode::Plain;
    type Response = response::Resume<u8, u8>;
}

impl<'d> Plain<'d> for Plaintext {
    type Delivery = response::Delivery<u8>;

    fn parse<'id, 'e, 'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        &mut self,
        input: codec::Input<'id, 'input, 'd, R>,
        _: &mut turns::Context<'_, 'e, 'd, Self::ConnState, Self::Send>,
    ) -> Result<codec::Parse<'id, u8>, scope::Error>
    where
        'd: 'input,
    {
        if let Some(parsed) = &self.parsed {
            self.events.record(Event::Parse {
                pointer: input.as_ptr(),
                len: input.len(),
            });
            parsed.hit();
        }
        self.codec().parse(input)
    }

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        head: u8,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> Self::Delivery
    where
        'd: 'input,
    {
        if self
            .parsed
            .as_ref()
            .is_some_and(|parsed| parsed.hits() == 2)
        {
            ctx.wake_target().wake();
            return response::Delivery::RetryInput;
        }
        response::Delivery::Prepared(head)
    }

    fn reject(
        &mut self,
        cause: scope::Error,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> Self::Delivery {
        self.events.record(Event::Decode(cause));
        ctx.request(close::Directive::cut_final(close::Reason::Protocol));
        response::Delivery::Committed
    }
}

impl<'d> Resume<'d, u8, u8> for Plaintext {
    fn commit<A: work::Admission>(
        &mut self,
        item: response::Committed<u8>,
        _: &mut A,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<u8> {
        let byte = item.into_inner();
        self.events.record(Event::Head(byte));
        match self.mode {
            Mode::Ready => response::Settlement::Ready,
            Mode::Yield => response::Settlement::Yield(byte),
            Mode::Blocked => {
                ctx.wake_target().wake();
                response::Settlement::Blocked(byte)
            }
            Mode::Timeout => response::Settlement::Blocked(byte),
        }
    }

    fn settle<A: work::Admission>(
        &mut self,
        item: response::Item<u8>,
        _: &mut A,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Settlement<u8> {
        let byte = item.into_inner();
        let closing = matches!(ctx.close_status(), conn::CloseStatus::Closing(..));
        if matches!(self.mode, Mode::Timeout) && !closing {
            return response::Settlement::Blocked(byte);
        }
        self.events.record(Event::Settled {
            byte,
            closing,
            expired: ctx.state.is_some_and(|deadline| deadline.is_due(ctx.now())),
        });
        response::Settlement::Ready
    }
}

impl<'d> Output<'d> for Plaintext {}

impl<'d> policy::Compatible<'d, 'd, Fault, KeyTag<0>> for Plaintext {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self {
        self
    }

    fn terminal(&self) -> &Self {
        self
    }
}

impl<'d> policy::Compatible<'d, 'd, Identity, KeyTag<0>> for Plaintext {
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self {
        self
    }

    fn terminal(&self) -> &Self {
        self
    }
}

impl<'d> end::Sink<'d, 'd, Fault, KeyTag<0>, failure::Possible> for Plaintext {
    fn fail(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        cause: terminal::Error<'d, Fault>,
        _: &mut region::Token<region::Identity<'d>>,
    ) {
        let (cause, closing) = cause.into_parts();
        self.events.record(Event::Failed(cause, closing));
    }
}

impl<'d> session::Connected<'d> for Plaintext {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        *ctx.state = ctx.lifetime.expires_at();
        self.conn.hit();
    }
}

impl<'d> session::Failed<'d> for Plaintext {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.events.record(Event::Connect(cause));
    }
}

impl<'d> session::Exhausted<'d> for Plaintext {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.events.record(Event::Exhausted(cause));
    }
}

impl<'d> Retirement<'d> for Plaintext {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        cause: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.events.record(Event::Receive(cause));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut Self::ConnState,
        _: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        self.events.record(Event::Quarantined);
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn policy(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
        _: close::Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> close::Policy {
        close::Policy::Permanent
    }

    fn disconnect(&mut self, ctx: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.events.record(Event::Closed(ctx.reason()));
        self.closed.hit();
    }
}

impl<'d> Scheduling<'d> for Plaintext {}
impl<'d> Target<'d, KeyTag<0>, 1> for Plaintext {}
