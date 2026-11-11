use std::mem;

use crate::conn::output::control::{self, kind};
use crate::conn::stream::state::receive::credits;
use crate::conn::wire::ingress::frames;
use crate::wire::{frame, ranges::plan, varint};
use control::owner;

const CREDIT_DIRTY: u8 = 1;

pub(in crate::conn) struct State {
    delivered_to: u64,
    highest_offset: u64,
    final_size: Option<u64>,
    reset: bool,
    pub(in crate::conn) credit: credits::Window,
    pub(in crate::conn::stream::state::receive::recv) control_flags: u8,
    pub(in crate::conn) max_stream_data: Option<owner::Key<kind::StreamLimit>>,
    pub(in crate::conn) stop_sending: owner::Signal<kind::StopSending>,
}

impl State {
    pub(in crate::conn::stream::state::receive::recv) fn new() -> Self {
        Self {
            delivered_to: 0,
            highest_offset: 0,
            final_size: None,
            reset: false,
            credit: credits::Window::new(),
            control_flags: 0,
            max_stream_data: None,
            stop_sending: owner::Signal::new(),
        }
    }

    pub(in crate::conn) fn high(&self) -> u64 {
        self.highest_offset
    }

    pub(in crate::conn) const fn delivered_to(&self) -> u64 {
        self.delivered_to
    }

    pub(in crate::conn) fn is_eof(&self) -> bool {
        matches!(self.final_size, Some(size) if size == self.delivered_to)
    }

    pub(in crate::conn) fn final_size(&self) -> Option<u64> {
        self.final_size
    }

    pub(in crate::conn) fn reset(&mut self, end: u64) {
        self.reset = true;
        self.final_size = Some(end);
        self.highest_offset = self.highest_offset.max(end);
    }

    pub(in crate::conn) fn is_reset(&self) -> bool {
        self.reset
    }

    pub(in crate::conn::stream::state::receive::recv) fn record(
        &mut self,
        end: u64,
        boundary: frame::Boundary,
    ) {
        self.highest_offset = self.highest_offset.max(end);
        if matches!(boundary, frame::Boundary::End) {
            self.final_size = Some(end);
        }
    }

    pub(in crate::conn::stream::state::receive::recv) fn deliver(&mut self, amount: usize) {
        if let Some(end) = self.delivered_to.checked_add(amount as u64) {
            self.delivered_to = end;
        }
    }

    pub(in crate::conn::stream::state::receive::recv) fn recycle(&mut self) {
        self.delivered_to = 0;
        self.highest_offset = 0;
        self.final_size = None;
        self.reset = false;
    }

    pub(in crate::conn) fn observe(
        &mut self,
        frames::CommitPermit { .. }: frames::CommitPermit<'_>,
        offset: varint::VarInt,
        record: plan::Record,
        boundary: frame::Boundary,
    ) {
        self.record(record.end(offset), boundary);
    }

    pub(in crate::conn) fn mark_credit(&mut self) {
        self.control_flags |= CREDIT_DIRTY;
    }

    pub(in crate::conn) fn clear_credit(&mut self) {
        self.control_flags &= !CREDIT_DIRTY;
    }

    pub(in crate::conn) fn has_credit(&self) -> bool {
        self.control_flags & CREDIT_DIRTY != 0
    }

    pub(in crate::conn) fn has_deferred(&self) -> bool {
        self.has_credit() || self.stop_sending.is_deferred()
    }

    pub(in crate::conn) fn retire(&mut self, ctrl: &mut control::Pending) {
        ctrl.remove(&mut self.max_stream_data);
        ctrl.clear(&mut self.stop_sending);
        self.control_flags = 0;
    }
}

const _: () = assert!(mem::size_of::<State>() == 9 * mem::size_of::<u64>());
