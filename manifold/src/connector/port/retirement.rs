use dope_core::driver::schedule;
use dope_net::link::egress::{self, data, metadata::arena};
use o3::cell::region;

use crate::{connector::port::state, dispatch::typed::identity};

pub(super) struct Retirement<'entry, 'token, 'd, B, I: identity::Identity> {
    slot: arena::Slot<'entry, 'd, B, state::Entry<'d, I>>,
    region: &'token mut region::Token<'d>,
    next: state::Availability,
}

impl<'entry, 'token, 'd, B: data::Payload<'d>, I: identity::Identity>
    Retirement<'entry, 'token, 'd, B, I>
{
    pub(super) fn begin(
        slot: arena::Slot<'entry, 'd, B, state::Entry<'d, I>>,
        connection: I,
        region: &'token mut region::Token<'d>,
    ) -> Option<Self> {
        let next = slot.state().begin_retirement(connection)?;
        Some(Self { slot, region, next })
    }

    pub(super) fn clear<'turn>(
        self,
        work: schedule::Application<'turn, 'd>,
    ) -> egress::ClearProgress {
        let Self { slot, region, next } = self;
        match slot.clear_step(region, work) {
            arena::Progress::Done => {
                slot.state().finish_retirement(next);
                egress::ClearProgress::Done
            }
            arena::Progress::Retry => egress::ClearProgress::Retry,
        }
    }
}
