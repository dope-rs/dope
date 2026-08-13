use dope_core::{
    driver::{flight, retained, route},
    io::transfer,
};
use dope_net::{
    link::slot::types,
    wire::{self, reclaim},
};

use crate::listener::{connection, writer::resources::Access as _};

#[derive(Clone, Copy, Default)]
pub(in crate::listener) struct PlainCursor {
    pub(in crate::listener) header_start: usize,
    pub(in crate::listener) header_end: usize,
    pub(in crate::listener) body_start: usize,
}

impl PlainCursor {
    pub(in crate::listener) const fn header_start(self) -> usize {
        self.header_start
    }

    pub(in crate::listener) const fn header_end(self) -> usize {
        self.header_end
    }

    pub(in crate::listener) const fn body_start(self) -> usize {
        self.body_start
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::listener) struct Written(usize);

impl Written {
    fn checked(capacity: usize, written: usize) -> Option<Self> {
        (written <= capacity).then_some(Self(written))
    }

    pub(in crate::listener) fn get(self) -> usize {
        self.0
    }

    pub(in crate::listener) fn prefix(self, bytes: &[u8]) -> &[u8] {
        &bytes[..self.0]
    }
}

pub(in crate::listener) enum Flow {
    Clear,
    Inflight,
    Plain,
    Stalled,
    Held,
    Deferred,
}

pub(in crate::listener) struct Handoff {
    pub(in crate::listener) armed_send: bool,
    pub(in crate::listener) restage: bool,
}

pub(in crate::listener) trait SlotFlow<'d, const ID: u8> {
    fn flow(&self, deferred: bool) -> Flow;
    fn after_handoff(&mut self, deferred: bool) -> Handoff;
    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> Option<Written>;
    fn hand_plain(
        &mut self,
        written: Written,
        flights: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
    fn resume_send(
        &mut self,
        flights: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    );
}

impl<'d, const ID: u8, W: wire::Wire, C> SlotFlow<'d, ID>
    for types::Connection<'d, ID, W, connection::State<'d, ID, C>>
{
    fn flow(&self, deferred: bool) -> Flow {
        use dope_net::link::slot::send;
        if self.send_status().inflight() {
            Flow::Inflight
        } else if self.state.send.has_remaining() {
            if <W::Reclaim as reclaim::Policy>::ON_SUBMIT
                && !self.state.send.has_inflight_plain()
                && self.send_status().retention() == send::Retention::Clear
            {
                Flow::Stalled
            } else {
                Flow::Plain
            }
        } else if self.send_status().retention() == send::Retention::Held {
            Flow::Held
        } else if deferred {
            Flow::Deferred
        } else {
            Flow::Clear
        }
    }

    fn after_handoff(&mut self, deferred: bool) -> Handoff {
        let armed = self.send_status().inflight();
        let restage = matches!(self.flow(deferred), Flow::Plain | Flow::Deferred);
        Handoff {
            armed_send: armed,
            restage,
        }
    }

    fn accept_write(&mut self, write_buf_cap: usize, written: usize) -> Option<Written> {
        let Some(written) = Written::checked(write_buf_cap, written) else {
            self.set_close_after();
            return None;
        };
        Some(written)
    }

    fn hand_plain(
        &mut self,
        written: Written,
        flights: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let (state, mut sending) = self.split_direct_sending();
        let Some(mut direct) = state.send.direct() else {
            sending.abort();
            return;
        };
        let was_inflight = sending.inflight();
        let consumed = sending.submit_plain(flights, driver, direct.flight_mut().plain(written));
        let armed = sending.inflight() && !was_inflight;
        direct.record_handoff(consumed, armed);
    }

    fn resume_send(
        &mut self,
        flights: &flight::Slots<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let (state, mut sending) = self.split_direct_sending();
        let Some(mut direct) = state.send.direct() else {
            sending.abort();
            return;
        };
        let was_inflight = sending.inflight();
        let cursor = direct.cursor();
        let limit = transfer::Len::clamp(direct.remaining());
        let vectored = direct.flight_mut().vectored(cursor, limit);
        let consumed = sending.submit_vectored(vectored, flights, driver);
        let armed = sending.inflight() && !was_inflight;
        direct.record_handoff(consumed, armed);
    }
}
