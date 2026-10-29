use dope_core::io::transfer;
use dope_net::wire::send::{self, progress::Plan, reclaim};
use dope_test::harness::observe::Trace;
use o3::buffer::write::{Sink as _, consume};

use crate::common;
pub(crate) struct Buffer<'a, const CAP: usize> {
    pub(crate) send: send::Storage<'a, common::Buffer<CAP>>,
    faults: &'a Trace<consume::Error, 16>,
}

impl<'a, const CAP: usize> Buffer<'a, CAP> {
    pub(crate) fn new(
        send: send::Storage<'a, common::Buffer<CAP>>,
        faults: &'a Trace<consume::Error, 16>,
    ) -> Self {
        Self { send, faults }
    }

    pub(crate) fn plain(
        self,
        plain: send::Plain<'a>,
    ) -> send::Prepared<'a, reclaim::OnSubmit, send::Plain<'a>> {
        let mut plan = Plan::plain(self.send, plain);
        let (store, mut input) = plan.parts();
        let part = input.take(transfer::Len::prefix(
            store.bytes.available(),
            transfer::Len::MAX,
        ));
        if let Err(error) = store.bytes.write_slice(part.as_slice()) {
            self.faults.record(error);
        }
        part.commit();
        plan.finish().buffered()
    }

    pub(crate) fn vectored<const N: usize>(
        self,
        plain: send::Vectored<'a, N>,
    ) -> send::Prepared<'a, reclaim::OnSubmit, send::Plain<'a>> {
        let mut plan = Plan::vectored(self.send, &plain);
        let (store, mut input) = plan.parts();
        let mut parts = input.take(transfer::Len::prefix(
            store.bytes.available(),
            input.remaining(),
        ));
        for part in parts.take() {
            if let Err(error) = store.bytes.write_slice(part.bytes()) {
                self.faults.record(error);
            }
        }
        parts.commit();
        plan.finish().buffered()
    }

    pub(crate) fn complete(
        done: send::Completion<'a, reclaim::OnSubmit, common::Buffer<CAP>>,
    ) -> send::Transition<'a, reclaim::OnSubmit, send::Plain<'a>> {
        let send = done.consume_max();
        send::Transition::unchanged(send.buffered())
    }
}
