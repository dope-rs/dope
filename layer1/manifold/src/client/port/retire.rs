use dope_core::driver::schedule::app;
use dope_net::link::egress::{self, data, metadata::arena};
use o3::base::cell::region;

use crate::{app::dispatch::identity, client::port::state};

pub(super) struct Retire<'entry, 'token, 'e, 'd, B, I: identity::Identity> {
    slot: arena::Slot<'entry, 'e, 'd, B, state::Entry<'d, I>>,
    region: &'token mut region::Token<region::Identity<'d>>,
    drain: state::Drain<'entry, 'd, I>,
}

impl<'entry, 'token, 'e, 'd, B: data::raw::Payload<'d>, I: identity::Identity>
    Retire<'entry, 'token, 'e, 'd, B, I>
{
    pub(super) fn begin(
        slot: arena::Slot<'entry, 'e, 'd, B, state::Entry<'d, I>>,
        conn: I,
        region: &'token mut region::Token<region::Identity<'d>>,
    ) -> Result<Option<Self>, state::Busy> {
        Ok(slot.state().begin_retire(conn)?.map(|drain| Self {
            slot,
            region,
            drain,
        }))
    }

    pub(super) fn clear<'turn>(
        self,
        work: app::Work<'turn, 'd>,
        cancel: impl FnOnce(B),
    ) -> egress::Cleared {
        let Self {
            slot,
            region,
            drain,
        } = self;
        match slot.clear_step(region, work, cancel) {
            arena::Progress::Done => {
                drain.finish();
                egress::Cleared::Done
            }
            arena::Progress::Retry => egress::Cleared::Retry,
        }
    }
}
