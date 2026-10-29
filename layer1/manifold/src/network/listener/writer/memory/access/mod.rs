use std::mem;

use dope_core::{
    driver::{
        result,
        route::{Identity, KeyTag},
    },
    io::{socket::msg, transfer, transfer::progress},
};
use dope_net::{
    link::{self, egress, egress::data, event},
    wire::send,
};
use o3::{
    buffer::{resident, segments::scatter},
    depot::array,
};
pub(in crate::network::listener::writer) use step::{Finish, Step};

use crate::network::listener::{
    self, conn,
    writer::{memory, memory::release},
};

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
            State::Active(_) | State::Failed(_) | State::Rejected(_) => Motion::Empty,
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

pub(in crate::network::listener) struct Direct<'p, 's, 'd, ID: Identity, B> {
    lease: release::Lease<'p, 's, 'd, memory::Flight<B>, ID>,
    head: memory::Len,
    pub(in crate::network::listener::writer) progress: Progress,
}

impl<'p, 's, 'd, ID: Identity, B> Direct<'p, 's, 'd, ID, B> {
    /// Preserves the accepted total: header bytes plus the unchanged source length.
    pub(in crate::network::listener) fn new(
        mut slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        head: memory::Len,
        total: transfer::NonZero,
        source: Option<B>,
    ) -> Self {
        let flight = slot.0.get_mut().project();
        *flight.source = source;
        *flight.position = scatter::raw::Position::new();
        Self {
            lease: slot.0,
            head,
            progress: Progress::new(total),
        }
    }

    pub(in crate::network::listener) fn body(
        slot: memory::DirectSlot<'p, 's, 'd, ID, B>,
        body: egress::NonEmpty<B>,
    ) -> Self {
        let total = body.geometry().wire();
        Self::new(slot, memory::Len::ZERO, total, Some(body.into_inner()))
    }

    pub(in crate::network::listener::writer) fn take_source(&mut self) -> Option<B> {
        self.lease.get_mut().project().source.take()
    }

    pub(in crate::network::listener::writer) fn prepare(&mut self) -> Result<Prepared<'_>, Fault>
    where
        B: data::raw::Payload<'d>,
    {
        let State::Active(active) = &self.progress.0 else {
            return Ok(Prepared::Idle);
        };
        let end = self.head.transfer();
        let consumed = active.sent.len();
        let (start, offset) = if consumed < end {
            (consumed, transfer::Len::ZERO)
        } else {
            (end, consumed.abs_diff(end))
        };
        let left = active.sent.left();
        let this = self.lease.get_mut().project();
        if start == transfer::Len::ZERO && left == end {
            // SAFETY: Len proves the prefix fits this exact fixed header owner.
            let bytes = unsafe {
                this.header
                    .into_ref()
                    .as_slice()
                    .get_unchecked(..end.into_raw())
            };
            // SAFETY: Flight and its pool lease stay pinned through completion.
            let plain = unsafe { send::raw::Plain::retain(bytes) };
            return Ok(Prepared::Plain(plain, Step::new(&mut self.progress)));
        }
        let limit = left;
        // SAFETY: the cursor is derived from this header's accepted progress.
        let header = unsafe {
            this.header
                .into_ref()
                .as_slice()
                .get_unchecked(start.into_raw()..end.into_raw())
        };
        let mut sent = progress::State::new(limit);
        let mut count = array::Len::<{ listener::IOV_CAP }>::ZERO;
        if !header.is_empty() {
            let step = sent.reserve(transfer::Len::prefix(header.len(), sent.left()));
            let len = step.amount().into_raw();
            // SAFETY: the step is bounded by header and the descriptor owner.
            let bytes = unsafe { header.get_unchecked(..len) };
            // SAFETY: the header owner stays pinned for the flight, and `bytes`
            // is a committed in-bounds prefix of it.
            let iovec = unsafe { msg::raw::Iovec::retain(bytes) };
            this.iovecs[0] = iovec;
            count = array::Len::ONE;
            step.commit();
        }
        if !sent.is_empty() {
            let Some(source) = this.source.as_ref() else {
                return Err(Fault::new(offset, transfer::Len::ZERO, sent.left()));
            };
            // SAFETY: the accepted total is header plus unchanged source length.
            // Confirmed progress stays within that total, so positive sent.left()
            // after the header proves offset is inside this source. Speculative
            // iovecs never move the checkpoint past that confirmed offset.
            let mut chunks =
                unsafe { source.resume(this.position, data::Offset::from_len(offset)) };
            while let Some(vacant) = count.vacancy() {
                let left = sent.left();
                if left.is_zero() {
                    break;
                }
                let Some(chunk) = sent.scatter(&mut chunks) else {
                    let extent = transfer::Len::prefix(source.len(), transfer::Len::MAX);
                    return Err(Fault::new(extent.abs_diff(left), extent, left));
                };
                // SAFETY: the direct lease keeps this source chunk fixed through
                // terminal completion or quiescence.
                let iovec = unsafe { msg::raw::Iovec::retain(chunk.bytes()) };
                *vacant.get_mut(this.iovecs) = iovec;
                count = vacant.commit();
            }
        }
        let bytes = sent.len();
        // SAFETY: Flight pins every descriptor and source owner until terminal
        // completion or quiescence.
        let raw = unsafe {
            msg::raw::Vectored::retain((&mut *this.message, &*this.iovecs, count, bytes))
        };
        Ok(Prepared::Many(
            send::Vectored::from_message(raw),
            Step::new(&mut self.progress),
        ))
    }
}

impl<B, ID: Identity> Drop for Direct<'_, '_, '_, ID, B> {
    fn drop(&mut self) {
        *self.lease.get_mut().project().source = None;
    }
}

const _: () = assert!(
    mem::size_of::<Direct<'static, 'static, 'static, KeyTag<0>, resident::Shared<'static>>>()
        == 5 * mem::size_of::<usize>()
);
