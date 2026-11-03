use crate::stream::release;
use o3::buffer;

use std::time;

use crate::conn::output::{
    control::journal::probes,
    transmit::permit::{self, fit},
};
use crate::wire::packet::header;
use crate::{conn, stream};

pub(in crate::conn) mod ack;
pub(in crate::conn) mod delivery;
pub(in crate::conn) mod early;
pub(in crate::conn) mod epochs;
mod losses;
pub(in crate::conn) mod shift;
pub(in crate::conn) mod timer;

#[derive(Clone, Copy, Default)]
pub(in crate::conn) struct Streak {
    first: Option<(header::Number, time::Instant)>,
    last_pn: Option<header::Number>,
}

pub(super) enum Finding {
    Pending,
    Persistent,
}

impl Streak {
    pub(super) fn acknowledged(&mut self, pn: u64) {
        if self.first.is_some_and(|(first, _)| pn >= first.get()) {
            self.first = None;
        }
    }

    pub(super) fn lost(
        &mut self,
        pn: header::Number,
        sent: time::Instant,
        elicits: bool,
        ack_after: bool,
        limit: time::Duration,
    ) -> Finding {
        // Out-of-order losses cannot move this retained run backwards.
        if self.last_pn.is_some_and(|previous| pn <= previous) {
            return Finding::Pending;
        }
        self.last_pn = Some(pn);
        let finding = if elicits {
            let first = self.first.get_or_insert((pn, sent)).1;
            if sent.saturating_duration_since(first) >= limit {
                self.first = None;
                Finding::Persistent
            } else {
                Finding::Pending
            }
        } else {
            Finding::Pending
        };
        if ack_after {
            // For this owner p, its ACK a and any later loss q across a,
            // p < a < q < largest_acked forces p out first by threshold 3.
            self.first = None;
        }
        finding
    }
}

const _: () = assert!(crate::proto::recovery::rtt::PACKET_THRESHOLD <= 3);

pub struct Progress<'a, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> {
    machine: &'a mut conn::Machine<DOMAIN, B, D>,
}

impl<'a, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment> Progress<'a, DOMAIN, B, D> {
    pub(in crate::conn) fn new(machine: &'a mut conn::Machine<DOMAIN, B, D>) -> Self {
        Self { machine }
    }

    pub fn try_loss(&mut self, now: time::Instant) -> Result<(), conn::Timing> {
        self.try_advance(now)
            .map_err(|error| self.machine.fail_timing(error))
    }

    fn try_advance(&mut self, now: time::Instant) -> Result<(), conn::Timing> {
        if self.machine.try_terminal(now)? {
            return Ok(());
        }
        if let Some(idle) = self.machine.egress.recovery.timer.idle
            && now >= idle
        {
            self.machine.close_now();
            self.machine.peer.reconcile.insert(conn::Effects::LIFECYCLE);
            return Ok(());
        }
        let Some(deadline) = fit::loss_deadline(&permit::Permit::new(self.machine)) else {
            return Ok(());
        };
        if now < deadline {
            return Ok(());
        }

        let conn::Machine {
            egress,
            control,
            handshake: tls,
            streams,
            ..
        } = &mut *self.machine;
        let mut delivery = delivery::Delivery::new(
            delivery::Graph::new(
                egress,
                control,
                tls,
                &mut streams.state,
                &mut streams.events,
            ),
            release::Ignore,
        );
        let outcome = losses::Detector::new(&mut delivery).try_run(now)?;
        if matches!(outcome, losses::Outcome::Clear)
            && let Some(epoch) = delivery.egress.recovery.timer.pto_epoch
        {
            delivery.egress.recovery.probe.epoch = Some(epoch);
            delivery.egress.recovery.probe.allowance.arm();
            delivery.handshake.crypto.arm_probes(epoch);
            if epoch == conn::Epoch::Application {
                probes::Probes::new(&mut *delivery.control).arm(epoch);
                delivery.streams.transmit.arm_probes();
            }
            delivery.egress.recovery.probe.count.increment();
        }
        delivery.try_update(now)
    }
}
