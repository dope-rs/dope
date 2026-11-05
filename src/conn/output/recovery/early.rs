use crate::stream::release;
use o3::buffer;

use std::time;

use crate::conn::output::{self, recovery::delivery};
use crate::{conn, stream};

pub(in crate::conn) fn try_reject<'a, const DOMAIN: u8, B, D>(
    egress: &'a mut output::Egress,
    mut delivery: delivery::Delivery<'a, DOMAIN, B, D, release::Ignore>,
    now: time::Instant,
) -> Result<(), conn::Error>
where
    B: stream::Recv,
    D: buffer::raw::Segment,
{
    if conn::Epoch::Application
        .bind(&egress.recovery.spaces)
        .largest_acked
        .is_some()
    {
        return Err(conn::Error::ProtocolViolation);
    }
    egress
        .recovery
        .packet_journals
        .space_mut(conn::Epoch::Application)
        .drain_where(
            |journal| journal.transmission.is_early(),
            |journal, controls, streams| {
                delivery.lose(journal, controls, streams);
            },
        );
    delivery.streams.transmit.reject_early(delivery.control)?;
    delivery.try_update(egress, now).map_err(conn::Error::from)
}
