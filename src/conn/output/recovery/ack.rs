use crate::stream::release;
use o3::buffer;

use std::time;

use crate::conn::output::recovery::{Streak, delivery, losses, timer};
use crate::conn::output::{self, cc};
use crate::{conn, stream, wire::frame::ack::ranges};

pub(in crate::conn) struct Receipt<'a> {
    pub(in crate::conn) delay: time::Duration,
    pub(in crate::conn) ranges: ranges::Validated<'a>,
    pub(in crate::conn) advanced: bool,
}

pub(in crate::conn) fn try_apply<'a, const DOMAIN: u8, B, D, E>(
    egress: &'a mut output::Egress,
    mut delivery: delivery::Delivery<'a, DOMAIN, B, D>,
    epoch: conn::Epoch,
    receipt: Receipt<'_>,
    now: time::Instant,
    evidence: &mut E,
) -> Result<(), conn::Error>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    E: release::Sink,
{
    let Receipt {
        delay,
        ranges,
        advanced: mut changed,
    } = receipt;
    let largest = ranges.largest();
    let valid = timer::Timer::is_validated(egress, delivery.streams.local());
    let mut sample = None;
    let mut elicits = false;
    let (result, boundary) = egress.recovery.packet_journals.space_mut(epoch).try_ack(
        ranges,
        |journal, controls, streams| {
            changed = true;
            let elicit = journal.transmission.is_eliciting();
            elicits |= elicit;
            if journal.pn.get() == largest {
                sample = Some(now.saturating_duration_since(journal.sent_time));
            }
            egress.cc.packet_acked(
                journal.bytes_sent as u64,
                journal.sent_time,
                journal.transmission.is_flight(),
                cc::Packet::from_epoch(epoch, journal.pn),
            );
            delivery.ack(journal, controls, streams, evidence)
        },
    );
    if valid && elicits {
        egress.recovery.probe.count.reset();
    }
    if let Some(boundary) = boundary {
        epoch
            .bind_mut(&mut egress.recovery.streaks)
            .acknowledged(boundary);
    }
    result?;
    if elicits && let Some(sample) = sample {
        let recovery = &mut egress.recovery;
        if recovery.rtt.update(sample, delay) {
            for (streak, space) in recovery.streaks.iter_mut().zip(&recovery.spaces) {
                *streak = Streak {
                    first: None,
                    next_pn: space.next_pn,
                };
            }
        }
    }
    let outcome = losses::try_run(egress, &mut delivery, now, changed)?;
    if changed || matches!(outcome, losses::Outcome::Lost) {
        delivery.try_update(egress, now)?;
    }
    Ok(())
}
