use std::{net, process};

use collections::queue::fixed;
use dope_core::{
    driver::{
        flight, retained,
        route::{self, table},
        schedule,
    },
    io::{
        datagram,
        fd::handles,
        recv,
        socket::{self, msg},
    },
};
use o3::{buffer::pool, cell::region, collections, mem::credit};

mod sealed;

pub(super) use sealed::Bounded;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Ready,
    Deferred,
    Stopped,
}

impl Phase {
    fn accepts_work(self) -> bool {
        self != Self::Stopped
    }

    fn can_submit(self) -> bool {
        self == Self::Ready
    }

    fn begin_pass(&mut self) -> bool {
        match self {
            Self::Ready => true,
            Self::Deferred => {
                *self = Self::Ready;
                true
            }
            Self::Stopped => false,
        }
    }
}

pub(super) enum Payload<'d> {
    Owned(Bounded<Vec<u8>>),
    OwnedSuffix(Bounded<crate::datagram::OwnedSuffix>),
    Buffer(Bounded<pool::Cursor>),
    Packet(Bounded<recv::View<'d>>),
    RetainedPacket(Bounded<recv::Retained<'d>>),
}

impl Payload<'_> {
    fn parts(&self) -> msg::Parts<'_, 1> {
        let part = match self {
            Self::Owned(payload) => payload.part(),
            Self::OwnedSuffix(payload) => payload.part(),
            Self::Buffer(payload) => payload.part(),
            Self::Packet(packet) => packet.part(),
            Self::RetainedPacket(packet) => packet.part(),
        };
        msg::Parts::single(part)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub(super) struct ResidentBytes(usize);

pub(super) trait ResidentPayload: AsRef<[u8]> + Sized {
    fn resident_bytes(&self, receive_slot: ResidentBytes) -> ResidentBytes;
}

impl ResidentBytes {
    pub(super) const fn new(bytes: usize) -> Self {
        Self(bytes)
    }
}

impl ResidentPayload for Vec<u8> {
    fn resident_bytes(&self, _receive_slot: ResidentBytes) -> ResidentBytes {
        ResidentBytes(self.capacity())
    }
}

impl ResidentPayload for crate::datagram::OwnedSuffix {
    fn resident_bytes(&self, _receive_slot: ResidentBytes) -> ResidentBytes {
        ResidentBytes(self.storage.capacity())
    }
}

impl ResidentPayload for pool::Cursor {
    fn resident_bytes(&self, _receive_slot: ResidentBytes) -> ResidentBytes {
        ResidentBytes(self.len() + self.spare_capacity())
    }
}

impl ResidentPayload for recv::View<'_> {
    fn resident_bytes(&self, receive_slot: ResidentBytes) -> ResidentBytes {
        receive_slot
    }
}

impl ResidentPayload for recv::Retained<'_> {
    fn resident_bytes(&self, receive_slot: ResidentBytes) -> ResidentBytes {
        receive_slot
    }
}

struct Retained<T> {
    value: T,
    bytes: ResidentBytes,
}

impl<T> Retained<T> {
    fn settle(self, ledger: &credit::Ledger) -> T {
        ledger.release(self.bytes.0);
        self.value
    }
}

impl<'d> Retained<Payload<'d>> {
    fn retire(self, ledger: &credit::Ledger) -> Option<Vec<u8>> {
        match self.settle(ledger) {
            Payload::Packet(_) | Payload::RetainedPacket(_) | Payload::Buffer(_) => None,
            Payload::Owned(payload) => Some(payload.into_inner()),
            Payload::OwnedSuffix(payload) => Some(payload.into_inner().into_storage()),
        }
    }
}

struct Queued<'d> {
    addr: net::SocketAddr,
    retained: Retained<Payload<'d>>,
    segment: Option<datagram::GsoSegment>,
}

impl<'d> Queued<'d> {
    fn new(
        retained: Retained<Payload<'d>>,
        addr: net::SocketAddr,
        segment: Option<datagram::GsoSegment>,
    ) -> Self {
        Self {
            addr,
            retained,
            segment,
        }
    }
}

pub(super) struct Send<'d> {
    retained: Retained<Payload<'d>>,
    target: net::SocketAddr,
    addr: socket::raw::Inet,
    iovs: [msg::Iovec; 1],
    msg: msg::Header,
    control: Option<datagram::GsoControl>,
    flight: Option<flight::Flight<'d>>,
}

impl<'d> Send<'d> {
    fn new(queued: Queued<'d>) -> Self {
        let target = queued.addr;
        let control = queued.segment.map(datagram::GsoControl::new);
        Self {
            retained: queued.retained,
            target,
            addr: socket::raw::Inet::from_std(target),
            iovs: [msg::Iovec::empty()],
            msg: msg::Header::new(),
            control,
            flight: None,
        }
    }

    pub(super) fn fill_message<'a>(&'a mut self) -> msg::Message<'a> {
        let Self {
            retained,
            addr,
            iovs,
            msg,
            control,
            ..
        } = self;
        let payload: &'a Payload<'d> = &retained.value;
        let parts = payload.parts();
        let mut message = msg::Builder::new(msg);
        message.name(addr);
        if let Some(control) = control {
            control.attach(&mut message);
        }
        message.finish(iovs, parts).message()
    }

    fn into_queued(self) -> Queued<'d> {
        Queued {
            addr: self.target,
            retained: self.retained,
            segment: self.control.map(datagram::GsoControl::into_segment),
        }
    }

    fn finish(mut self) -> Retained<Payload<'d>> {
        let Some(flight) = self.flight.take() else {
            process::abort();
        };
        let _ = flight.complete();
        self.retained
    }
}

pub(super) struct Sender<'d, const ID: u8> {
    pending: fixed::Fifo<Queued<'d>>,
    in_flight: table::Slab<Send<'d>, route::KeyTag<ID, { route::SEND }>>,
    ledger: credit::Ledger,
    receive_slot: ResidentBytes,
    flights: flight::Slots<'d, super::SendTag<ID>>,
    phase: Phase,
}

impl<'d, const ID: u8> Sender<'d, ID> {
    pub(super) fn try_new(
        pending: usize,
        limit: ResidentBytes,
        in_flight: table::Capacity,
        receive_slot: ResidentBytes,
        flights: flight::Slots<'d, super::SendTag<ID>>,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            pending: fixed::Fifo::try_with_capacity(pending)?,
            in_flight: table::Slab::try_with_capacity(in_flight)?,
            ledger: credit::Ledger::new(limit.0),
            receive_slot,
            flights,
            phase: Phase::Ready,
        })
    }

    pub(super) fn accepts_work(&self) -> bool {
        self.phase.accepts_work()
    }

    pub(super) fn try_enqueue<P, F>(
        &mut self,
        payload: P,
        addr: net::SocketAddr,
        segment: Option<datagram::GsoSegment>,
        into_payload: F,
    ) -> Result<(), P>
    where
        P: ResidentPayload,
        F: FnOnce(Bounded<P>) -> Payload<'d>,
    {
        if !self.phase.accepts_work() {
            return Err(payload);
        }
        let Self {
            pending,
            ledger,
            receive_slot,
            ..
        } = self;
        let Some(entry) = pending.vacant_entry() else {
            return Err(payload);
        };
        let resident = payload.resident_bytes(*receive_slot);
        let payload = Bounded::try_new(payload)?;
        if !ledger.try_acquire(resident.0) {
            return Err(payload.into_inner());
        }
        let retained = Retained {
            value: into_payload(payload),
            bytes: resident,
        };
        entry.push_back(Queued::new(retained, addr, segment));
        Ok(())
    }

    pub(super) fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        if !self.phase.accepts_work() {
            return if !self.pending.is_empty() {
                schedule::Progress::Runnable
            } else if !self.in_flight.is_empty() {
                schedule::Progress::waiting(region)
            } else {
                schedule::Progress::Quiescent
            };
        }
        if !self.pending.is_empty()
            && self.phase.can_submit()
            && self.in_flight.len() < self.in_flight.capacity().get()
        {
            schedule::Progress::Runnable
        } else if !self.pending.is_empty() || !self.in_flight.is_empty() {
            schedule::Progress::waiting(region)
        } else {
            schedule::Progress::Quiescent
        }
    }

    pub(super) fn flush<'turn, 'owner>(
        &mut self,
        fd: &handles::DatagramDescriptor<'d>,
        work: schedule::Application<'turn, 'd>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
    ) where
        'd: 'owner,
    {
        let Self {
            pending,
            in_flight,
            flights,
            phase,
            ..
        } = self;
        if !phase.begin_pass() {
            return;
        }
        while !pending.is_empty() {
            let Some(slot) = in_flight.vacant_entry() else {
                break;
            };
            if !work.take() {
                break;
            }
            let Some((queued, vacancy)) = pending.pop_front_reserved() else {
                break;
            };
            let mut occupied = slot.insert_occupied(Send::new(queued));
            let key = occupied.key();
            match super::Submission.send(fd, flights, occupied.get_mut(), key, driver) {
                Ok(flight) => occupied.get_mut().flight = Some(flight),
                Err(_) => {
                    *phase = Phase::Deferred;
                    vacancy.push_front(occupied.remove().into_queued());
                    break;
                }
            }
        }
    }

    pub(super) fn complete(&mut self, token: route::Token) -> Option<Vec<u8>> {
        token
            .parts::<route::KeyTag<ID, { route::SEND }>>()
            .and_then(|parts| self.in_flight.remove_parts(parts))?
            .finish()
            .retire(&self.ledger)
    }

    pub(super) fn drain(
        &mut self,
        work: schedule::Maintenance<'_, 'd>,
        mut recycle: impl FnMut(Vec<u8>),
    ) {
        while !self.pending.is_empty() && work.take() {
            let Some(queued) = self.pending.pop_front() else {
                break;
            };
            if let Some(payload) = queued.retained.retire(&self.ledger) {
                recycle(payload);
            }
        }
    }

    pub(super) fn stop(&mut self) {
        self.phase = Phase::Stopped;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.in_flight.is_empty()
    }
}

const _: () = assert!(size_of::<Bounded<Vec<u8>>>() == size_of::<Vec<u8>>());
const _: () = assert!(
    size_of::<Bounded<crate::datagram::OwnedSuffix>>() == size_of::<crate::datagram::OwnedSuffix>()
);
const _: () = assert!(size_of::<Bounded<pool::Cursor>>() == size_of::<pool::Cursor>());
const _: () = assert!(size_of::<Bounded<recv::View<'_>>>() == size_of::<recv::View<'_>>());
