use std::{mem, num::NonZero, ops::ControlFlow};

use o3::{
    alloc,
    buffer::{self, write},
    depot::{self, queue::pool},
};

use crate::{
    conn::Appended,
    errors,
    stream::{outbound, release, send},
    wire::{frame, stream::Id, varint},
};

use outbound::{
    Finish, chunk,
    chunk::prefix::{Cursor, Prefix},
};
pub(crate) use plans::{Prepared, View, Written};
use release::batch;

mod plans;

pub(crate) enum Acked<T> {
    Live(T),
    Closed(T),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Range {
    encoded_offset: u64,
    end: varint::VarInt,
}

/// Exclusive admission while the sender still accepts bytes and its final mark.
pub(crate) struct Open<'a>(&'a mut Sender);

pub(crate) struct Append<'a> {
    open: Open<'a>,
    pending: varint::Span,
}

struct Source<'lane, 'sender, O: buffer::raw::Segment> {
    sender: &'sender mut Sender,
    chunks: pool::Lane<'lane, chunk::Node<O>>,
    amount: NonZero<usize>,
}

pub(super) struct Head<'lane, 'sender, O: buffer::raw::Segment> {
    entry: chunk::Front<'lane, O>,
    front: &'sender mut Cursor,
    amount: NonZero<usize>,
}

impl<'lane, 'sender, O: buffer::raw::Segment> From<Source<'lane, 'sender, O>>
    for Head<'lane, 'sender, O>
{
    fn from(source: Source<'lane, 'sender, O>) -> Self {
        // SAFETY: the private source retains the admitted prefix in this exact lane.
        let (entry, (sender, amount)) = unsafe {
            depot::raw::Project::<(), pool::Head>::project(
                (source.chunks, (source.sender, source.amount)),
                (),
            )
        };
        Head {
            entry,
            front: &mut sender.front,
            amount,
        }
    }
}

impl<'lane, 'sender, O: buffer::raw::Segment> Head<'lane, 'sender, O> {
    pub(super) fn into_parts(
        self,
    ) -> (chunk::Front<'lane, O>, &'sender mut Cursor, NonZero<usize>) {
        (self.entry, self.front, self.amount)
    }
}

#[derive(Debug)]
pub(crate) struct Acknowledged;

/// Retains the unacknowledged sender while its bytes are readmitted as fresh.
#[must_use]
pub(crate) struct Replay<'a> {
    sender: &'a mut Sender,
    bytes: usize,
}

impl Replay<'_> {
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn commit(self) {
        self.sender.pending = varint::Span::from_zero(self.sender.pending.end());
        if matches!(self.sender.finish, Finish::Sent) {
            self.sender.finish = Finish::Marked;
        }
    }
}

impl Range {
    const FIN: u64 = 1 << 63;

    pub(crate) fn write(
        self,
        out: &mut write::Append<'_>,
        limit: usize,
        id: Id,
        copy: impl FnOnce(Range, &mut write::Append<'_>) -> Result<Option<Appended>, alloc::Error>,
    ) -> Result<Option<(NonZero<u16>, u16)>, alloc::Error> {
        let room = limit.saturating_sub(out.len());
        let mut out = out.append();
        let len = self.len_usize();
        let boundary = match self.boundary() {
            send::Boundary::More => frame::Boundary::More,
            send::Boundary::End => frame::Boundary::End,
        };
        let Some(frame::StreamHead) =
            frame::Frame::write_stream(&mut out, id, self.offset(), boundary, len)?
        else {
            return Ok(None);
        };
        if out.len().saturating_add(len) <= room {
            let Some(_) = copy(self, &mut out)? else {
                out.rollback();
                return Ok(None);
            };
            let wire_len = u16::try_from(out.len()).ok().and_then(NonZero::<u16>::new);
            if wire_len.is_none() {
                out.rollback();
            }
            Ok(wire_len.map(|wire_len| (wire_len, len as u16)))
        } else {
            out.rollback();
            Ok(None)
        }
    }

    pub(crate) const fn offset(self) -> u64 {
        self.encoded_offset & !Self::FIN
    }

    /// The immutable range retains the extent admitted by `Open::plan_append`.
    pub(crate) const fn end(self) -> u64 {
        self.end.get()
    }

    pub(crate) const fn end_offset(self) -> varint::VarInt {
        self.end
    }

    pub(crate) const fn len(self) -> u64 {
        self.end() - self.offset()
    }

    pub(crate) const fn len_usize(self) -> usize {
        self.len() as usize
    }

    pub(crate) const fn boundary(self) -> send::Boundary {
        if self.encoded_offset & Self::FIN == 0 {
            send::Boundary::More
        } else {
            send::Boundary::End
        }
    }
}

impl<'a> Open<'a> {
    /// Admits one suffix against the remaining offset and allocation domains.
    /// The exclusive loan retains the existing extent and this checked addition.
    pub(crate) fn plan_append(self, bytes: usize) -> Option<Append<'a>> {
        let pending = self.0.pending.try_extend(bytes as u64)?;
        Some(Append {
            open: self,
            pending,
        })
    }

    pub(crate) fn mark_fin(self) {
        self.0.finish = Finish::Marked;
    }
}

impl Append<'_> {
    pub(crate) fn commit(self, boundary: send::Boundary) {
        self.open.0.pending = self.pending;
        if matches!(boundary, send::Boundary::End) {
            self.open.mark_fin();
        }
    }
}

pub(crate) struct Sender {
    front: Cursor,
    base_offset: varint::VarInt,
    pending: varint::Span,
    pub(super) finish: Finish,
    stop_error: Option<varint::Stored>,
    reset_error: Option<varint::Stored>,
}

impl Default for Sender {
    fn default() -> Self {
        Self {
            front: Cursor::default(),
            base_offset: varint::VarInt::ZERO,
            pending: varint::Span::ZERO,
            finish: Finish::More,
            stop_error: None,
            reset_error: None,
        }
    }
}

impl Sender {
    pub(crate) fn head_offset(&self) -> u64 {
        self.base_offset.get() - self.front.offset() as u64
    }

    pub(crate) const fn base_offset(&self) -> u64 {
        self.base_offset.get()
    }

    pub(crate) fn buffered_len(&self) -> usize {
        (self.pending.end().get() - self.base_offset.get()) as usize
    }

    pub(crate) fn open(&mut self) -> Option<Open<'_>> {
        match self.finish {
            Finish::More => Some(Open(self)),
            Finish::Marked | Finish::Sent | Finish::Acked => None,
        }
    }

    pub(crate) fn view(&self) -> View {
        View {
            pending: self.pending,
            finish: self.finish,
        }
    }

    pub(crate) const fn finish(&self) -> Finish {
        self.finish
    }

    pub(crate) fn stop_error(&self) -> Option<errors::Code> {
        self.stop_error
            .map(|value| errors::Code::from_varint(value.get()))
    }

    pub(crate) fn reset_error(&self) -> Option<errors::Code> {
        self.reset_error
            .map(|value| errors::Code::from_varint(value.get()))
    }

    pub(crate) fn publish(commit: crate::conn::Publication<'_>) {
        let (sender, written) = commit.into_parts();
        sender.pending = written.pending;
        if matches!(written.boundary, send::Boundary::End) {
            sender.finish = Finish::Sent;
        }
    }

    pub(crate) fn try_replay(&mut self) -> Result<Replay<'_>, Acknowledged> {
        if self.base_offset != varint::VarInt::ZERO || matches!(self.finish, Finish::Acked) {
            return Err(Acknowledged);
        }
        let bytes = self.buffered_len() - self.view().unsent_len();
        Ok(Replay {
            sender: self,
            bytes,
        })
    }

    /// A rejected early reset retains no byte owners and restarts its final size.
    pub(crate) fn reject_reset(&mut self) {
        self.front = Cursor::default();
        self.base_offset = varint::VarInt::ZERO;
        self.pending = varint::Span::ZERO;
        self.finish = Finish::More;
    }

    pub(crate) fn ack<E: release::Sink, O: buffer::raw::Segment>(
        reclaim: crate::conn::Reclaim<'_, O>,
    ) -> Acked<batch::Batch<E::Mode>> {
        let (sender, mut chunks, buffers, run) = reclaim.into_parts();
        let len = run.len();
        let finish = match run.boundary() {
            send::Boundary::More => sender.finish,
            send::Boundary::End => Finish::Acked,
        };
        let base = run.end();
        let mut rest = NonZero::new(len);
        let mut released = batch::Batch::<E::Mode>::wire_bytes(len);
        while let Some(left) = rest {
            let head = Source {
                sender: &mut *sender,
                chunks,
                amount: left,
            }
            .into();
            let (front, left) = match Prefix::new(head) {
                ControlFlow::Break(prefix) => {
                    released.retire(prefix);
                    break;
                }
                ControlFlow::Continue(rest) => rest,
            };
            rest = left;
            sender.front = Cursor::default();
            let (owner, lane) = front.take();
            chunks = lane;
            released.release(owner, buffers);
        }
        let buffered = sender.buffered_len() - len;
        sender.base_offset = base;
        sender.finish = finish;
        if matches!(finish, Finish::Acked) && buffered == 0 {
            Acked::Closed(released)
        } else {
            Acked::Live(released)
        }
    }

    pub(crate) fn stop<E: release::Sink>(
        &mut self,
        chunks: &mut pool::Lane<'_, chunk::Node<impl buffer::raw::Segment>>,
        buffers: &mut outbound::BufferCache,
        code: errors::Code,
    ) -> batch::Batch<E::Mode> {
        let end = self.view().next_offset();
        self.stop_error = Some((*code.as_varint()).into());
        let released = self.discard_all::<E>(chunks, buffers);
        self.base_offset = end;
        released
    }

    /// Drains once and retains the first reset code across repeated peer stops.
    pub(crate) fn stop_reset<E: release::Sink>(
        &mut self,
        chunks: &mut pool::Lane<'_, chunk::Node<impl buffer::raw::Segment>>,
        buffers: &mut outbound::BufferCache,
        code: errors::Code,
    ) -> (batch::Batch<E::Mode>, bool) {
        let released = self.stop::<E>(chunks, buffers, code);
        let reset = self.reset_error.is_none();
        if reset {
            self.reset_error = Some((*code.as_varint()).into());
        }
        (released, reset)
    }

    pub(crate) fn reset_sent<E: release::Sink>(
        &mut self,
        chunks: &mut pool::Lane<'_, chunk::Node<impl buffer::raw::Segment>>,
        buffers: &mut outbound::BufferCache,
        code: errors::Code,
    ) -> batch::Batch<E::Mode> {
        let end = self.view().next_offset();
        let released = self.discard_all::<E>(chunks, buffers);
        self.base_offset = end;
        self.reset_error = Some((*code.as_varint()).into());
        released
    }

    fn discard_all<E: release::Sink>(
        &mut self,
        chunks: &mut pool::Lane<'_, chunk::Node<impl buffer::raw::Segment>>,
        buffers: &mut outbound::BufferCache,
    ) -> batch::Batch<E::Mode> {
        let mut released = batch::Batch::<E::Mode>::wire_bytes(self.buffered_len());
        while let Some(owner) = chunks.pop_front() {
            released.release(owner, buffers);
        }
        self.pending = varint::Span::empty(self.pending.start());
        self.front = Cursor::default();
        released
    }
}

const _: () = assert!(mem::size_of::<Sender>() == 7 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Range>() == 2 * mem::size_of::<u64>());
const _: () = assert!(mem::size_of::<Open<'static>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Append<'static>>() == 3 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Written>() == 3 * mem::size_of::<usize>());
const _: () = assert!(usize::BITS <= u64::BITS);
const _: () = assert!(mem::size_of::<View>() == mem::size_of::<(u64, usize, Finish)>());
