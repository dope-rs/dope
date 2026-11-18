use std::mem;

use dope_core::{
    driver::{
        result,
        route::{Identity, KeyTag},
    },
    io::{transfer, transfer::progress},
};
use dope_net::{
    link::{self, egress, egress::data, event},
    wire::send,
};
use o3::buffer::resident;
pub(in crate::network::listener::writer) use step::Step;

use crate::network::listener::{
    self, conn,
    writer::{memory, memory::release, state::Send},
};

mod raw;
mod step;

pub(in crate::network::listener::writer) struct Progress(State);

#[repr(u8)]
pub(in crate::network::listener::writer) enum Arm {
    Idle,
    Armed,
}

enum State {
    Active(Active),
    Failed(Fault),
    Rejected(event::SendFailure<result::SubmitError>),
}

struct Active {
    motion: Motion,
    sent: progress::State,
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(in crate::network::listener::writer) enum Motion {
    Empty,
    Done,
    Ready,
    Waiting,
    Consumed,
}

#[derive(Clone, Copy)]
pub(in crate::network::listener::writer) struct Fault {
    offset: transfer::Len,
    length: transfer::Len,
    remaining: transfer::Len,
}

pub(in crate::network::listener::writer) enum Prepared<'a> {
    Idle,
    Plain(send::Plain<'a>, Step<'a>),
    Many(send::Vectored<'a, { listener::IOV_CAP }>, Step<'a>),
}

impl Progress {
    pub(in crate::network::listener::writer) fn new(total: transfer::NonZero) -> Self {
        Self(State::Active(Active {
            motion: Motion::Ready,
            sent: progress::State::new(transfer::Len::from_nonzero(total)),
        }))
    }

    pub(in crate::network::listener::writer) fn invalidate(&mut self, fault: Fault) {
        self.0 = State::Failed(fault);
    }

    pub(in crate::network::listener::writer) fn reject(
        &mut self,
        failure: event::SendFailure<result::SubmitError>,
    ) {
        self.0 = State::Rejected(failure);
    }

    pub(in crate::network::listener::writer) fn error(&self) -> Option<conn::Transmit> {
        match &self.0 {
            State::Active(_) => None,
            State::Failed(fault) => Some(fault.transmit()),
            State::Rejected(failure) => Some(conn::Transmit::Rejected(*failure)),
        }
    }

    pub(in crate::network::listener::writer) fn settle(&mut self, done: Option<link::Consumed>) {
        let State::Active(active) = &mut self.0 else {
            return;
        };
        if let Some(done) = done {
            let next = match active.sent.try_extend(done.into_len()) {
                Ok(next) => next,
                Err(cause) => {
                    self.0 = State::Failed(Fault::new(
                        active.sent.len(),
                        cause.requested(),
                        cause.available(),
                    ));
                    return;
                }
            };
            active.sent = next;
        }
        active.motion = Motion::Ready;
    }

    pub(in crate::network::listener::writer) fn has_more(&self) -> bool {
        matches!(&self.0, State::Active(active) if !active.sent.is_empty())
    }

    pub(in crate::network::listener::writer) fn motion(&self) -> Motion {
        match &self.0 {
            State::Active(active) if !active.sent.is_empty() => active.motion,
            State::Active(_) => Motion::Done,
            State::Failed(_) | State::Rejected(_) => Motion::Empty,
        }
    }

    pub(in crate::network::listener::writer) fn record(&mut self, done: link::Consumed, arm: Arm) {
        let sent = done.into_len();
        self.settle(Some(done));
        let State::Active(active) = &mut self.0 else {
            return;
        };
        active.motion = match (arm, sent.nonzero()) {
            (Arm::Idle, None) => Motion::Waiting,
            (Arm::Idle, Some(_)) | (Arm::Armed, None) => Motion::Ready,
            (Arm::Armed, Some(_)) => Motion::Consumed,
        };
    }
}

impl Fault {
    fn new(offset: transfer::Len, length: transfer::Len, left: transfer::Len) -> Self {
        Self {
            offset,
            length,
            remaining: left,
        }
    }

    const fn transmit(self) -> conn::Transmit {
        conn::Transmit::Payload {
            offset: self.offset,
            length: self.length,
            remaining: self.remaining,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Fault>() == 3 * mem::size_of::<transfer::Len>());
    assert!(mem::size_of::<Arm>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Arm>() == mem::align_of::<bool>());
    assert!(mem::size_of::<Motion>() == mem::size_of::<u8>());
};

pub(in crate::network::listener) trait Body<B> {
    fn wire(&self) -> Option<transfer::NonZero>;
    fn into_value(self) -> Option<egress::load::Value<B>>;
    fn begin<'p, 's, 'd, ID: Identity>(
        self,
        send: &mut Send<'p, 's, 'd, ID, B>,
        slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        head: memory::Len,
        total: transfer::NonZero,
    )
    where
        B: data::raw::Payload<'d>;
}

impl<B> Body<B> for () {
    fn wire(&self) -> Option<transfer::NonZero> {
        None
    }

    fn into_value(self) -> Option<egress::load::Value<B>> {
        None
    }

    fn begin<'p, 's, 'd, ID: Identity>(
        self,
        send: &mut Send<'p, 's, 'd, ID, B>,
        slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        head: memory::Len,
        total: transfer::NonZero,
    )
    where
        B: data::raw::Payload<'d>,
    {
        send.begin(Direct::new(slot, head, total, Source::Header));
    }
}

impl<B> Body<B> for egress::NonEmpty<B> {
    fn wire(&self) -> Option<transfer::NonZero> {
        Some(self.geometry().wire())
    }

    fn into_value(self) -> Option<egress::load::Value<B>> {
        Some(egress::load::Value::Checked(self))
    }

    fn begin<'p, 's, 'd, ID: Identity>(
        self,
        send: &mut Send<'p, 's, 'd, ID, B>,
        slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        head: memory::Len,
        total: transfer::NonZero,
    )
    where
        B: data::raw::Payload<'d>,
    {
        send.begin(Direct::with_body(slot, head, total, self));
    }
}

pub(in crate::network::listener) struct Direct<
    'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>,
>
{
    lease: release::Lease<'p, 's, 'd, memory::Flight<B, B::Position>, ID>,
    head: memory::Len,
    source: Source,
    pub(in crate::network::listener::writer) progress: Progress,
}

enum Source {
    Header,
    Body,
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> Direct<'p, 's, 'd, ID, B> {
    fn new(
        slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        head: memory::Len,
        total: transfer::NonZero,
        source: Source,
    ) -> Self {
        Self {
            lease: slot,
            head,
            source,
            progress: Progress::new(total),
        }
    }
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> Direct<'p, 's, 'd, ID, B> {
    pub(in crate::network::listener) fn body(
        slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        body: egress::NonEmpty<B>,
    ) -> Self {
        let total = body.geometry().wire();
        Self::with_body(slot, memory::Len::ZERO, total, body)
    }

    fn with_body(
        mut slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        head: memory::Len,
        total: transfer::NonZero,
        body: egress::NonEmpty<B>,
    ) -> Self {
        let flight = slot.get_mut().project();
        flight.source.write(body.into_inner());
        *flight.position = B::Position::default();
        Self::new(slot, head, total, Source::Body)
    }
}

const _: () = assert!(
    mem::size_of::<Direct<'static, 'static, 'static, KeyTag<0>, resident::Shared<'static>>>()
        == 5 * mem::size_of::<usize>()
);
