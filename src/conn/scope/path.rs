use o3::buffer::{self, resident};

use crate::{conn, stream};
use conn::wire::path::probe;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Rotation {
    Queued,
    Unavailable,
}

/// Path validation and connection-ID commands.
#[repr(transparent)]
pub struct Path<
    'access,
    const DOMAIN: u8,
    B: stream::Recv = Vec<u8>,
    const RECONCILE: bool = false,
    D: buffer::raw::Segment = resident::Shared<'access>,
> {
    machine: &'access mut conn::Machine<DOMAIN, B, D>,
}

impl<const DOMAIN: u8, B: stream::Recv, const RECONCILE: bool, D: buffer::raw::Segment>
    Path<'_, DOMAIN, B, RECONCILE, D>
{
    pub(super) fn new(
        machine: &mut conn::Machine<DOMAIN, B, D>,
    ) -> Path<'_, DOMAIN, B, RECONCILE, D> {
        Path { machine }
    }

    /// Queues an application probe with a distinct token for each request.
    /// Cancel it when the application stops waiting for its response.
    pub fn try_probe(&mut self, data: [u8; 8]) -> Result<(), probe::Error> {
        let state = self.machine.egress.lifecycle.state;
        if state != conn::State::Established {
            return Err(probe::Error::State { state, data });
        }
        self.machine.control.queue().try_queue(data)?;
        self.machine.record::<RECONCILE>(conn::Effects::TRANSMIT);
        Ok(())
    }

    /// Releases an outstanding application probe, returning whether it existed.
    /// Later responses to its token are ignored. Migration probes stay owned
    /// by the connection and cannot be canceled here.
    pub fn cancel_probe(&mut self, data: [u8; 8]) -> bool {
        if !self.machine.control.queue().cancel(data) {
            return false;
        }
        self.machine.record::<RECONCILE>(conn::Effects::SCHEDULER);
        true
    }

    pub(crate) fn rotate(&mut self) -> Rotation {
        use conn::wire::path::local;

        if self.machine.egress.lifecycle.state != conn::State::Established {
            return Rotation::Unavailable;
        }
        match self.machine.path.local.rotate() {
            local::Rotation::Queued => {}
            local::Rotation::Unavailable => return Rotation::Unavailable,
        }
        let work = self.machine.path.local.work(&self.machine.control);
        self.machine.egress.derived.advertise(work.controls());
        self.machine
            .record::<RECONCILE>(conn::Effects::TRANSMIT | conn::Effects::ROUTING);
        Rotation::Queued
    }
}
