use dope_core::driver::route;
use o3::cell::region;

use crate::link::egress::{self, data, queue::flight};

pub(in crate::link) struct Transfer<'a, 'queue, 'd, const IOV: usize, B> {
    queue: &'a mut egress::Queue<'queue, 'd, IOV, B>,
}

impl<'a, 'queue, 'd, const IOV: usize, B: data::Payload<'d>> Transfer<'a, 'queue, 'd, IOV, B> {
    pub(super) fn new(queue: &'a mut egress::Queue<'queue, 'd, IOV, B>) -> Self {
        Self { queue }
    }

    pub(in crate::link) fn complete(
        &mut self,
        token: &mut region::Token<'d>,
        target: route::Token,
        bytes: usize,
    ) -> bool {
        let Some(released) = self.queue.flight.complete(target, bytes) else {
            return false;
        };
        let acknowledged = self.settle(token, released);
        debug_assert!(acknowledged, "a completed flight is an ACK prefix");
        acknowledged
    }

    pub(in crate::link) fn abort(&mut self, target: route::Token) -> bool {
        if !self.queue.flight.abort(target) {
            return false;
        }
        self.queue.counters.set_partial(0);
        true
    }

    pub(in crate::link) fn settle(
        &mut self,
        token: &mut region::Token<'d>,
        released: flight::Released<IOV>,
    ) -> bool {
        self.queue.ack(token, released)
    }

    pub(in crate::link) fn settle_submitted(
        &mut self,
        token: &mut region::Token<'d>,
        released: flight::Released<IOV>,
        bytes: usize,
    ) -> bool {
        self.settle(token, released) && self.record_submitted(bytes)
    }

    fn record_submitted(&self, bytes: usize) -> bool {
        let Some(total) = self.queue.counters.submitted().checked_add(bytes) else {
            return false;
        };
        self.queue.counters.set_submitted(total);
        true
    }

    pub(in crate::link) fn take_submitted(&self) -> usize {
        self.queue.counters.take_submitted()
    }
}
