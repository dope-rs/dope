use crate::conn::output::control::journal::records::Records;
use crate::conn::output::delivery;
use crate::conn::output::journals::stack;
use crate::stream::release;
use o3::buffer;
use std::time;

use crate::conn::output::journals::{controls, streams};
use crate::conn::output::{self, control, journals, recovery::timer};
use crate::conn::stream::event::queue::Pending;
use crate::conn::stream::{State, state::receive};
use crate::conn::wire::tls;
use crate::{conn, stream};
use control::revision;

/// One lifetime-bound borrow of the natural delivery owners and release evidence.
pub(in crate::conn) struct Delivery<
    'a,
    const DOMAIN: u8,
    B: stream::Recv,
    D: buffer::raw::Segment,
    E,
> {
    pub(super) control: &'a mut control::Pending,
    pub(super) handshake: &'a mut tls::Handshake<DOMAIN>,
    pub(super) streams: &'a mut State<B, D>,
    pub(super) stream_events: &'a mut Pending,
    pub(super) release_evidence: E,
}

impl<'a, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment, E>
    Delivery<'a, DOMAIN, B, D, E>
{
    pub(in crate::conn) fn new(
        ctrl: &'a mut control::Pending,
        hs: &'a mut tls::Handshake<DOMAIN>,
        state: &'a mut State<B, D>,
        events: &'a mut Pending,
        proof: E,
    ) -> Self {
        Self {
            control: ctrl,
            handshake: hs,
            streams: state,
            stream_events: events,
            release_evidence: proof,
        }
    }

    pub(super) fn try_update(
        &mut self,
        egress: &mut output::Egress,
        now: time::Instant,
    ) -> Result<(), conn::Timing> {
        timer::Timer::try_update(egress, &self.handshake.keys, self.streams.local(), now)
    }
}

impl<const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment, E: release::Sink>
    Delivery<'_, DOMAIN, B, D, E>
{
    pub(super) fn lose(
        &mut self,
        pkt: journals::Packet,
        ctrls: stack::raw::Drain<'_, revision::Key, controls::Tag>,
        sends: stack::raw::Drain<'_, delivery::Handle<delivery::Stream>, streams::Tag>,
    ) {
        if let Some(handle) = pkt.crypto {
            self.handshake.crypto.lose(handle);
        }
        for handle in ctrls {
            Records::new(&mut *self.control).lose(handle);
        }
        for handle in sends {
            self.streams.transmit.lose(handle);
        }
    }

    fn ack_ctrl(&mut self, handle: revision::Key) -> Result<(), conn::Error> {
        match Records::new(&mut *self.control).ack(handle) {
            control::Effect::None => Ok(()),
            control::Effect::RetireStream(id) => receive::Update::new(self.streams).retire_reset(
                self.stream_events,
                id,
                self.control,
            ),
        }
    }

    pub(super) fn ack(
        &mut self,
        pkt: journals::Packet,
        ctrls: stack::raw::Drain<'_, revision::Key, controls::Tag>,
        sends: stack::raw::Drain<'_, delivery::Handle<delivery::Stream>, streams::Tag>,
    ) -> Result<(), conn::Error> {
        if let Some(handle) = pkt.crypto {
            self.handshake.crypto.acknowledge(handle);
        }
        for handle in ctrls {
            self.ack_ctrl(handle)?;
        }
        for handle in sends {
            receive::Update::new(self.streams).acknowledge(
                self.stream_events,
                handle,
                self.control,
                &mut self.release_evidence,
            );
        }
        Ok(())
    }
}
