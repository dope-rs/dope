use std::{mem, time};

use o3::buffer::{self, resident};

use crate::conn::{self, Epoch};
use crate::stream;

/// Non-temporal recovery metrics safe for endpoint inspection.
#[repr(transparent)]
pub struct Metrics<
    'conn,
    const DOMAIN: u8 = 0,
    B: stream::Recv = Vec<u8>,
    D: buffer::raw::Segment = resident::Shared<'conn>,
> {
    connection: &'conn conn::Machine<DOMAIN, B, D>,
}

impl<'conn, const DOMAIN: u8, B: stream::Recv, D: buffer::raw::Segment>
    Metrics<'conn, DOMAIN, B, D>
{
    pub(crate) fn new(conn: &'conn conn::Machine<DOMAIN, B, D>) -> Self {
        Self { connection: conn }
    }

    pub fn window(&self) -> u64 {
        self.connection.egress.cc.metrics().window()
    }

    pub fn in_flight(&self) -> u64 {
        self.connection
            .egress
            .recovery
            .in_flight()
            .unwrap_or(u64::MAX)
    }

    pub fn slow_limit(&self) -> u64 {
        self.connection.egress.cc.metrics().slow_limit()
    }

    pub fn unacked(&self, space: Epoch) -> usize {
        self.connection
            .egress
            .recovery
            .packet_journals
            .space(space)
            .count()
    }

    pub fn smoothed_rtt(&self) -> Option<time::Duration> {
        self.connection.egress.recovery.rtt.smoothed_rtt
    }

    pub fn min_rtt(&self) -> Option<time::Duration> {
        self.connection.egress.recovery.rtt.min_rtt
    }
}

const _: () = {
    assert!(mem::size_of::<Metrics<'static, 0>>() == mem::size_of::<&conn::Machine<0, Vec<u8>>>());
    assert!(!mem::needs_drop::<Metrics<'static, 0>>());
};
