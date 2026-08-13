use dope_core::{
    driver::{self, schedule},
    io::socket::msg,
};
use o3::{cell::region, collections};

use crate::link::egress::{
    self, data,
    metadata::{self, pool},
    queue::{entry, flight, lanes},
};

struct IovCount<const N: usize>;

impl<const N: usize> IovCount<N> {
    const VALID: () = {
        assert!(N != 0, "egress IOV must be non-zero");
        assert!(
            N <= msg::MAX_IOVECS,
            "egress IOV exceeds the platform limit"
        );
    };
}

/// Pool-owned retained egress backing shared by generation-owned lanes.
pub(in crate::link) struct Storage<'d, B, const IOV: usize> {
    flights: flight::Pool<'d, IOV>,
    entries: pool::Pool<'d, entry::Entry<B>>,
}

impl<'d, B, const IOV: usize> Storage<'d, B, IOV> {
    pub(in crate::link) fn try_with_config(
        token: &region::Token<'d>,
        config: egress::Config,
        lanes: usize,
    ) -> Result<Self, collections::AllocationError> {
        use crate::link::egress::metadata::pool::Pool;
        let () = IovCount::<IOV>::VALID;
        assert!(lanes != 0, "egress storage requires at least one lane");
        Ok(Self {
            flights: flight::Pool::try_with_capacity(token, config.flight_capacity(lanes))?,
            entries: Pool::try_with_config(token, config, lanes)?,
        })
    }

    pub(in crate::link) fn lane(&self, index: usize) -> Option<lanes::Lane<'d, IOV>> {
        if !self.entries.contains_lane(index) {
            return None;
        }
        Some(lanes::Lane::new(self.entries.credit_state(index)))
    }

    pub(in crate::link) fn clear_step<'turn>(
        &mut self,
        lane: &mut lanes::Lane<'d, IOV>,
        work: schedule::Maintenance<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> egress::ClearProgress {
        if lane.is_active() {
            return egress::ClearProgress::Waiting;
        }
        let queue = metadata::Queue::with_lane(&self.entries, &lane.metadata);
        if queue.is_empty() {
            return egress::ClearProgress::Done;
        }
        let Some((permit, token)) = schedule::MaintenancePermit::try_take_with_region(work, driver)
        else {
            return egress::ClearProgress::Retry;
        };
        let _permit = permit;
        queue.clear_one(token);
        if queue.is_empty() {
            egress::ClearProgress::Done
        } else {
            egress::ClearProgress::Retry
        }
    }
}

impl<'d, B: data::Payload<'d>, const IOV: usize> Storage<'d, B, IOV> {
    pub(in crate::link) fn try_enqueue(
        &self,
        token: &mut region::Token<'d>,
        lane: &lanes::Lane<'d, IOV>,
        bytes: B,
    ) -> Result<(), B> {
        use crate::link::egress::Queue;
        Queue::<IOV, B>::enqueue(
            metadata::Queue::with_lane(&self.entries, &lane.metadata),
            token,
            bytes,
        )
    }

    pub(in crate::link) fn queue<'a>(
        &'a mut self,
        lane: &'a mut lanes::Lane<'d, IOV>,
    ) -> egress::Queue<'a, 'd, IOV, B> {
        let Self {
            flights, entries, ..
        } = self;
        lane.queue(entries, flights)
    }
}
