use std::{mem, num, pin};

use dope_net::link;

use crate::listener::writer::{self, flow};

#[derive(Default)]
pub(in crate::listener) struct Send<'d, const ID: u8> {
    direct: Option<DirectState<'d, ID>>,
}

struct DirectState<'d, const ID: u8> {
    flight: writer::DirectLease<'d, ID>,
    write_buf_len: usize,
    inflight_plain: usize,
    consumed_plain: usize,
    total_plain: num::NonZeroUsize,
}

pub(in crate::listener) struct Direct<'a, 'd, const ID: u8> {
    state: &'a mut DirectState<'d, ID>,
}

const _: () = assert!(mem::size_of::<DirectState<'static, 0>>() == 5 * mem::size_of::<usize>());
const _: () =
    assert!(mem::size_of::<Send<'static, 0>>() == mem::size_of::<DirectState<'static, 0>>());
const _: () = assert!(mem::size_of::<Direct<'static, 'static, 0>>() == mem::size_of::<usize>());

impl<'d, const ID: u8> Send<'d, ID> {
    pub(in crate::listener) fn begin(
        &mut self,
        write_buf_len: usize,
        total_plain: num::NonZeroUsize,
        flight: writer::DirectLease<'d, ID>,
    ) {
        self.direct = Some(DirectState {
            flight,
            write_buf_len,
            inflight_plain: 0,
            consumed_plain: 0,
            total_plain,
        });
    }

    /// Completes direct-send ownership and recycles its pinned storage.
    pub(in crate::listener) fn finish(&mut self) -> Option<usize> {
        let direct = self.direct.take()?;
        let total = direct.total_plain.get();
        drop(direct);
        Some(total)
    }

    pub(in crate::listener) fn direct(&mut self) -> Option<Direct<'_, 'd, ID>> {
        Some(Direct {
            state: self.direct.as_mut()?,
        })
    }

    /// Retires direct-send ownership after the net layer has quiesced its SQE.
    pub(in crate::listener) fn retire(&mut self) {
        drop(self.direct.take());
    }

    pub(in crate::listener) fn complete_handoff(&mut self, sent: link::Consumed) -> bool {
        let Some(direct) = self.direct.as_mut() else {
            return false;
        };
        let sent = sent.get();
        if sent > direct.inflight_plain {
            return false;
        }
        direct.consumed_plain -= direct.inflight_plain - sent;
        direct.inflight_plain = 0;
        true
    }

    pub(in crate::listener) fn has_remaining(&self) -> bool {
        self.direct
            .as_ref()
            .is_some_and(|direct| direct.consumed_plain < direct.total_plain.get())
    }

    pub(in crate::listener) fn has_inflight_plain(&self) -> bool {
        self.direct
            .as_ref()
            .is_some_and(|direct| direct.inflight_plain != 0)
    }

    pub(in crate::listener) const fn is_queue_path(&self) -> bool {
        self.direct.is_none()
    }
}

impl<'d, const ID: u8> Direct<'_, 'd, ID> {
    pub(in crate::listener) fn flight_mut(&mut self) -> pin::Pin<&mut writer::Flight<'d, ID>> {
        self.state.flight.flight_mut()
    }

    pub(in crate::listener) fn record_handoff(&mut self, consumed: link::Consumed, armed: bool) {
        let consumed = consumed.get();
        if armed {
            self.state.inflight_plain = consumed;
        }
        self.state.consumed_plain += consumed;
    }

    pub(in crate::listener) fn cursor(&self) -> flow::PlainCursor {
        let consumed = self.state.consumed_plain;
        flow::PlainCursor {
            header_start: consumed.min(self.state.write_buf_len),
            header_end: self.state.write_buf_len,
            body_start: consumed.saturating_sub(self.state.write_buf_len),
        }
    }

    pub(in crate::listener) fn remaining(&self) -> usize {
        self.state.total_plain.get() - self.state.consumed_plain
    }
}
