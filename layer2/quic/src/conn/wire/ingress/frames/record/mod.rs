use crate::errors::{self, detail};
use std::{num::NonZero, ops};

use crate::conn::stream::resident;
use crate::conn::stream::state::receive::input;
use crate::conn::wire::crypto::chunks;
use crate::wire::ranges::plan;
use crate::{
    conn,
    proto::recovery::pmtud,
    wire::{frame, packet::cid, stream, varint},
};
use conn::wire::ingress::frames;
use frame::{View, ack::ranges};
use plan::Record;

mod storage;

pub(super) use storage::{Control, Records, Vacant};
pub(in crate::conn) use storage::{Group, Index, Storage};

#[derive(Clone, Copy, Default)]
#[repr(transparent)]
pub(super) struct PayloadPlan(u64);

impl PayloadPlan {
    pub(super) const fn record(self) -> Record {
        Record::from_bits(self.0 as u32)
    }

    const fn start(self) -> usize {
        (self.0 >> u32::BITS) as u16 as usize
    }

    pub(super) fn range(self) -> ops::Range<usize> {
        let start = self.start();
        start..start + self.record().bytes()
    }

    fn set_record(&mut self, record: Record) {
        self.0 = u64::from(record.bits());
    }

    fn set_start(&mut self, start: usize) {
        self.0 = u64::from(self.0 as u32) | u64::from(start as u16) << u32::BITS;
    }
}

pub(super) type Parsed<'id> = View<ops::Range<usize>, ranges::Proof<'id>, cid::Id>;

/// A row's kind, immutable identity, and kind-specific admission remain together.
pub(in crate::conn) struct FrameRecord<'id> {
    id: stream::Id,
    kind: Kind<'id>,
}

enum Kind<'id> {
    Other(Parsed<'id>),
    Datagram(Datagram),
    Stream(StreamRecord),
    Reset(ResetRecord),
    Stop(StopRecord),
    Crypto(CryptoRecord<'id>),
}

struct Datagram {
    data: ops::Range<usize>,
    payload: Option<PayloadPlan>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Drop,
    Stream,
    Transient,
}

pub(super) struct StreamRecord {
    pub(super) offset: varint::VarInt,
    pub(super) data: ops::Range<usize>,
    pub(super) boundary: frame::Boundary,
    pub(super) key: Option<resident::Key>,
    pub(super) payload: PayloadPlan,
    admission: Status,
}

struct ResetRecord {
    code: errors::Code,
    end: varint::VarInt,
    key: Option<resident::Key>,
    free: Option<u64>,
}

struct StopRecord {
    code: errors::Code,
    key: Option<resident::Key>,
    admitted: bool,
}

struct CryptoRecord<'id> {
    offset: varint::VarInt,
    data: ops::Range<usize>,
    step: chunks::Step<'id>,
}

/// A typed loan can change only metadata belonging to its actual wire variant.
pub(in crate::conn) enum Receive<'a> {
    Stream(Stream<'a>),
    Reset(Reset<'a>),
}

pub(in crate::conn) struct Stream<'a>(&'a mut StreamRecord);
pub(in crate::conn) struct Reset<'a>(&'a mut ResetRecord);

pub(in crate::conn) struct Stored<'a> {
    offset: varint::VarInt,
    data: &'a ops::Range<usize>,
    payload: &'a mut PayloadPlan,
}

impl Stored<'_> {
    pub(in crate::conn) fn offset(&self) -> varint::VarInt {
        self.offset
    }
    pub(in crate::conn) fn data(&self) -> &ops::Range<usize> {
        self.data
    }
    pub(in crate::conn) fn record(&self) -> Record {
        self.payload.record()
    }
    pub(in crate::conn) fn set_start(&mut self, _: frames::CommitPermit<'_>, start: usize) {
        self.payload.set_start(start);
    }
}

impl Stream<'_> {
    pub(in crate::conn) fn offset(&self) -> varint::VarInt {
        self.0.offset
    }
    pub(in crate::conn) fn len(&self) -> usize {
        self.0.data.len()
    }
    pub(in crate::conn) fn boundary(&self) -> frame::Boundary {
        self.0.boundary
    }
    pub(in crate::conn) fn admit(&mut self, key: Option<resident::Key>, added: &plan::Added) {
        self.0.key = key;
        self.0.payload.set_record(added.record());
        self.0.admission = Status::Stream;
    }
    pub(in crate::conn) fn seen(&mut self, key: Option<resident::Key>, record: Record) {
        self.0.key = key;
        self.0.payload.set_record(record);
        self.0.admission = Status::Transient;
    }
    pub(in crate::conn) fn stored_mut(&mut self) -> Option<Stored<'_>> {
        if self.0.admission != Status::Stream {
            return None;
        }
        Some(Stored {
            offset: self.0.offset,
            data: &self.0.data,
            payload: &mut self.0.payload,
        })
    }
}

impl Reset<'_> {
    pub(in crate::conn) fn final_size(&self) -> varint::VarInt {
        self.0.end
    }
    pub(in crate::conn) fn admit(&mut self, key: Option<resident::Key>, free: u64) {
        self.0.key = key;
        self.0.free = Some(free);
    }
}

impl Datagram {
    fn stored_mut(&mut self) -> Option<Stored<'_>> {
        Some(Stored {
            offset: varint::VarInt::ZERO,
            data: &self.data,
            payload: self.payload.as_mut()?,
        })
    }
}

pub(in crate::conn) enum Operation {
    Drop,
    Datagram,
    Stream {
        id: stream::Id,
    },
    StreamTransient {
        key: Option<resident::Key>,
        id: stream::Id,
        offset: varint::VarInt,
        plan: Record,
        boundary: frame::Boundary,
    },
    Reset(input::Reset),
    Stop {
        key: Option<resident::Key>,
        id: stream::Id,
        error_code: errors::Code,
    },
}

/// Parsing accounts for payloads while the initialized record owner admits rows.
pub(super) struct Build<'slots> {
    pub(super) bytes: usize,
    dgrams: usize,
    max: Option<u64>,
    pub(super) close: Option<Index<'slots>>,
}

impl<'slots> Build<'slots> {
    pub(super) fn try_bytes(current: usize, add: usize) -> Result<usize, detail::Plan> {
        let limit = u16::MAX as usize;
        let Some(total) = current.checked_add(add) else {
            return Err(detail::Plan::Bytes {
                current,
                additional: add,
                limit,
            });
        };
        if total > limit {
            return Err(detail::Plan::Bytes {
                current,
                additional: add,
                limit,
            });
        }
        Ok(total)
    }

    pub(super) fn new(dgrams: usize, max: Option<u64>) -> Self {
        Self {
            bytes: 0,
            dgrams,
            max,
            close: None,
        }
    }

    fn try_datagram(
        &mut self,
        data: ops::Range<usize>,
        length: frame::Length,
        used: NonZero<usize>,
    ) -> Result<Datagram, detail::Plan> {
        let len = data.len();
        if self.max.is_none_or(|max| used.get() as u64 > max) {
            return Err(detail::Plan::Datagram {
                len,
                length,
                maximum: self.max,
            });
        }
        let payload = if self.dgrams == 0 {
            None
        } else {
            let record = Record::full(len).ok_or_else(|| detail::Payload::new(len))?;
            self.bytes = Self::try_bytes(self.bytes, len)?;
            self.dgrams -= 1;
            Some(PayloadPlan(u64::from(record.bits())))
        };
        Ok(Datagram { data, payload })
    }

    pub(super) fn try_record<'id>(
        &mut self,
        slot: Vacant<'_, '_, 'slots, 'id, '_, '_, '_, '_>,
        body: &frame::Body<'_, 'id>,
        packet: frames::PacketKind,
        local: stream::Initiator,
        frame: frame::Frame<ops::Range<usize>, ranges::Proof<'id>>,
        used: NonZero<usize>,
    ) -> Result<(), detail::Plan> {
        if !packet.allows(&frame, local) {
            return Err(conn::Error::ProtocolViolation.into());
        }
        let zero = stream::Id::from_varint(varint::VarInt::ZERO);
        let (id, kind) = match frame.into_view() {
            View::Stream {
                stream_id: id,
                offset,
                boundary,
                data,
                ..
            } => (
                id,
                Kind::Stream(StreamRecord {
                    offset,
                    data,
                    boundary,
                    key: None,
                    payload: PayloadPlan::default(),
                    admission: Status::Drop,
                }),
            ),
            View::ResetStream {
                stream_id: id,
                error_code: code,
                final_size: end,
            } => (
                id,
                Kind::Reset(ResetRecord {
                    code,
                    end,
                    key: None,
                    free: None,
                }),
            ),
            View::StopSending {
                stream_id: id,
                error_code: code,
            } => (
                id,
                Kind::Stop(StopRecord {
                    code,
                    key: None,
                    admitted: false,
                }),
            ),
            View::Datagram { data } => (
                zero,
                Kind::Datagram(self.try_datagram(data, frame::Length::Implicit, used)?),
            ),
            View::DatagramLen { data, .. } => (
                zero,
                Kind::Datagram(self.try_datagram(data, frame::Length::Explicit, used)?),
            ),
            View::Crypto { offset, data, .. } => (
                zero,
                Kind::Crypto(CryptoRecord {
                    offset,
                    data,
                    step: chunks::Step::new(body),
                }),
            ),
            wire => {
                let id = match &wire {
                    View::MaxStreamData { stream_id: id, .. } => *id,
                    _ => zero,
                };
                (id, Kind::Other(wire))
            }
        };
        let close = matches!(&kind, Kind::Other(View::ConnectionClose { .. }));
        let index = slot.insert(FrameRecord { id, kind }, packet.is_app())?;
        if close {
            self.close.get_or_insert(index);
        }
        Ok(())
    }
}

impl<'id> FrameRecord<'id> {
    pub(super) fn control(&self) -> Option<&Parsed<'id>> {
        match &self.kind {
            Kind::Other(wire) => Some(wire),
            _ => None,
        }
    }

    pub(super) fn datagram(&self) -> Option<(&ops::Range<usize>, PayloadPlan)> {
        let Kind::Datagram(record) = &self.kind else {
            return None;
        };
        Some((&record.data, record.payload?))
    }

    pub(super) fn stored_stream(&self) -> Option<&StreamRecord> {
        match &self.kind {
            Kind::Stream(stream) if stream.admission == Status::Stream => Some(stream),
            _ => None,
        }
    }

    fn is_receive_stream(&self) -> bool {
        matches!(&self.kind, Kind::Stream(record) if record.admission != Status::Drop)
    }

    pub(super) fn stored_dgram(&mut self) -> Option<Stored<'_>> {
        let Kind::Datagram(record) = &mut self.kind else {
            return None;
        };
        record.stored_mut()
    }

    pub(in crate::conn) fn operation(&self) -> Operation {
        let id = self.id;
        match &self.kind {
            Kind::Datagram(record) if record.payload.is_some() => Operation::Datagram,
            Kind::Stream(record) => match record.admission {
                Status::Drop => Operation::Drop,
                Status::Stream => Operation::Stream { id },
                Status::Transient => Operation::StreamTransient {
                    key: record.key,
                    id,
                    offset: record.offset,
                    plan: record.payload.record(),
                    boundary: record.boundary,
                },
            },
            Kind::Reset(record) => match record.free {
                Some(free) => Operation::Reset(input::Reset {
                    key: record.key,
                    id,
                    code: record.code,
                    end: record.end.get(),
                    free,
                }),
                None => Operation::Drop,
            },
            Kind::Stop(record) if record.admitted => Operation::Stop {
                key: record.key,
                id,
                error_code: record.code,
            },
            Kind::Datagram(_) | Kind::Stop(_) | Kind::Other(_) | Kind::Crypto(_) => Operation::Drop,
        }
    }

    fn receive(&mut self) -> Option<Receive<'_>> {
        match &mut self.kind {
            Kind::Stream(record) => Some(Receive::Stream(Stream(record))),
            Kind::Reset(record) => Some(Receive::Reset(Reset(record))),
            _ => None,
        }
    }
}

const _: () = assert!(pmtud::MAX_PMTU <= u16::MAX as u64);
