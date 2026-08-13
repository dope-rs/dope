use crate::{
    backend,
    driver::{self, ops::retirements, schedule},
    platform,
};

pub(crate) enum Returned {}
pub(crate) enum Fair {}

/// The only authority handed from common reactor maintenance to a backend.
pub(crate) struct Backend<'turn, 'd> {
    work: schedule::Reactor<'turn, 'd>,
}

impl<'turn, 'd> Backend<'turn, 'd> {
    pub(crate) fn prepare(
        context: &mut driver::Context<'_, 'd>,
        work: schedule::Reactor<'turn, 'd>,
    ) -> Self
    where
        backend::Backend: platform::Buffer,
    {
        Returned::reclaim(context, schedule::MAX_TURN_WORK_BUDGET);

        let total = work.remaining();
        let cursor = work.cursor();
        let backend_reserve = Fair::share::<{ schedule::REACTOR_LANES as usize }>(total, 1, cursor);

        let retirement_limit = work.remaining().saturating_sub(backend_reserve);
        {
            let mut retirement = work.budget::<retirements::Retirement>(retirement_limit);
            retirement.reclaim(context);
        }

        Self { work }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.work.remaining()
    }

    pub(crate) fn budget<Lane>(&self, limit: usize) -> schedule::Budget<'_, 'd, Lane> {
        self.work.budget(limit)
    }
}

impl Fair {
    pub(crate) const fn share<const PARTICIPANTS: usize>(
        total: usize,
        lane: usize,
        cursor: usize,
    ) -> usize {
        assert!(PARTICIPANTS != 0);
        assert!(lane < PARTICIPANTS);
        let base = total / PARTICIPANTS;
        let extra = total % PARTICIPANTS;
        let distance = (lane + PARTICIPANTS - (cursor % PARTICIPANTS)) % PARTICIPANTS;
        base + if distance < extra { 1 } else { 0 }
    }
}

impl Returned {
    fn reclaim(context: &mut driver::Context<'_, '_>, limit: usize)
    where
        backend::Backend: platform::Buffer,
    {
        let mut reclaimed = 0;
        for _ in 0..limit {
            let Some(returned) = context.pop_returned_buffer() else {
                break;
            };
            platform::Buffer::release(context.backend(), returned);
            reclaimed += 1;
        }
        if reclaimed != 0 {
            context
                .driver_ref()
                .credits()
                .release_recv_buffers(reclaimed);
        }
    }

    pub(in crate::driver) fn reclaim_all(context: &mut driver::Context<'_, '_>)
    where
        backend::Backend: platform::Buffer,
    {
        while let Some(returned) = context.pop_returned_buffer() {
            platform::Buffer::release(context.backend(), returned);
        }
    }
}
