use crate::stream::release;
use o3::buffer;

use std::time;

use crate::conn::output::{
    self, cc,
    recovery::{self, delivery},
};
use crate::proto::recovery::rtt;
use crate::{conn, stream};

#[derive(Default)]
struct Lost {
    any: bool,
    latest_sent: Option<time::Instant>,
    persistent: bool,
}

pub(super) enum Outcome {
    Clear,
    Lost,
}

pub(super) fn try_run<const DOMAIN: u8, B, D, E>(
    egress: &mut output::Egress,
    delivery: &mut delivery::Delivery<'_, DOMAIN, B, D, E>,
    now: time::Instant,
    changed: bool,
) -> Result<Outcome, conn::Timing>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
    E: release::Sink,
{
    let recovery = &egress.recovery;
    let largest = conn::Epoch::ALL.map(|epoch| {
        let packets = recovery.packet_journals.space(epoch);
        if packets.count() == 0 {
            None
        } else {
            epoch.bind(&recovery.spaces).largest_acked
        }
    });
    let mut admitted = [None; 3];
    let mut cutoff = None;
    if largest.iter().any(Option::is_some) {
        cutoff = now.checked_sub(recovery.rtt.try_delay()?);
        let pto = recovery.rtt.try_pto(time::Duration::ZERO)?;
        let mut periods = [None; 2];
        for ((epoch, largest), admitted) in
            conn::Epoch::ALL.into_iter().zip(largest).zip(&mut admitted)
        {
            let Some(largest) = largest else { continue };
            let with_ack =
                epoch == conn::Epoch::Application && !recovery.peer_max_ack_delay.is_zero();
            let period = match periods[with_ack as usize] {
                Some(period) => period,
                None => {
                    let period = if with_ack {
                        conn::Timing::try_add(pto, recovery.peer_max_ack_delay)?
                    } else {
                        pto
                    };
                    let period = conn::Timing::try_mul(period, 3)?;
                    periods[with_ack as usize] = Some(period);
                    period
                }
            };
            *admitted = Some((largest, period));
        }
    }
    let output::Egress { cc, recovery, .. } = egress;
    let output::Recovery {
        packet_journals,
        streaks,
        ..
    } = recovery;
    let mut lost = Lost::default();
    for (epoch, admitted) in conn::Epoch::ALL.into_iter().zip(admitted) {
        let Some((largest, period)) = admitted else {
            continue;
        };
        packet_journals.space_mut(epoch).drain_lost(
            largest,
            cutoff,
            |journal, controls, streams| {
                let pn = journal.pn;
                let sent = journal.sent_time;
                let flight = journal.transmission.is_flight();
                let probe =
                    cc::Packet::from_epoch(epoch, pn).and_then(|packet| cc.packet_lost(packet));
                let reduce = flight && probe.is_none();
                let elicits = journal.transmission.is_eliciting() && reduce;
                if matches!(
                    epoch.bind_mut(streaks).lost(
                        pn,
                        sent,
                        elicits,
                        journal.transmission.has_ack_after(),
                        period
                    ),
                    recovery::Finding::Persistent
                ) {
                    lost.persistent = true;
                }
                delivery.lose(journal, controls, streams);
                lost.any = true;
                if reduce {
                    lost.latest_sent =
                        Some(lost.latest_sent.map_or(sent, |latest| latest.max(sent)));
                }
            },
        );
    }
    if let Some(latest) = lost.latest_sent {
        cc.packets_lost(latest, now);
    }
    if lost.persistent {
        cc.collapse();
    }
    if changed || lost.any {
        let smoothed = recovery.rtt.smoothed_rtt.unwrap_or(rtt::INITIAL_RTT);
        cc.finish(now, smoothed);
    }
    Ok(if lost.any {
        Outcome::Lost
    } else {
        Outcome::Clear
    })
}
