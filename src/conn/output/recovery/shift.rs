use o3::buffer;

use std::time;

use crate::conn::output::{self, recovery::delivery};
use crate::proto::recovery::rtt;
use crate::{conn, stream};

/// Retransmits old-path flight before installing fresh recovery state.
pub(in crate::conn) fn try_commit<'a, const DOMAIN: u8, B, D>(
    egress: &'a mut output::Egress,
    mut delivery: delivery::Delivery<'a, DOMAIN, B, D>,
    now: time::Instant,
) -> Result<(), conn::Timing>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
{
    let epoch = conn::Epoch::Application;
    egress
        .recovery
        .packet_journals
        .space_mut(epoch)
        .drain_where(
            |_| true,
            |journal, controls, streams| {
                delivery.lose(journal, controls, streams);
            },
        );
    egress.recovery.probe.count.reset();
    egress.recovery.probe.allowance.clear();
    egress.recovery.probe.epoch = None;
    *epoch.bind_mut(&mut egress.recovery.streaks) = Default::default();
    egress.recovery.rtt = rtt::RttTracker::default();
    egress.cc.restart_path();
    egress.activity.peer_address_validated = true;
    delivery.try_update(egress, now)
}
