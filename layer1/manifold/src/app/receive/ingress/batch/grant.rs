use std::mem::size_of;

use dope_core::driver::{
    retained,
    schedule::{
        self,
        app::batch::{Bound, Reserved},
        ready,
    },
};
use dope_net::wire::{self, receive::batch};
use o3::mem;

use crate::app::receive::ingress::{Admission, Replay};

pub(crate) struct Grant<'a, 'c, 'owner, 'd: 'owner, W: wire::Spec> {
    driver: &'a mut retained::Context<'c, 'owner, 'd>,
    turn: schedule::Turn<'a, 'd>,
    reservation: Reserved<'a, 'd, batch::Capacity<'d, W>>,
}

impl<'a, 'c: 'a, 'owner: 'a, 'd: 'a + 'c + 'owner, W: wire::Spec + 'd> Admission<'a, 'c, 'owner, 'd>
    for Grant<'a, 'c, 'owner, 'd, W>
{
    fn fresh(
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> schedule::Capacity<Self> {
        match Self::reserve(turn, false) {
            schedule::Capacity::Available(held) => {
                schedule::Capacity::Available(Self::new(driver, turn, held))
            }
            schedule::Capacity::Exhausted => schedule::Capacity::Exhausted,
        }
    }

    fn replay(
        replay: Replay,
        ready: ready::Key<'d>,
        turn: schedule::Turn<'a, 'd>,
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
    ) -> schedule::Capacity<Self> {
        match Self::reserve(turn, !matches!(replay, Replay::First)) {
            schedule::Capacity::Available(held) => {
                schedule::Capacity::Available(Self::new(driver, turn, held))
            }
            schedule::Capacity::Exhausted => {
                driver.driver_ref().ready().activate(ready);
                schedule::Capacity::Exhausted
            }
        }
    }
}

impl<'a, 'c, 'owner, 'd: 'owner, W: wire::Spec + 'd> Grant<'a, 'c, 'owner, 'd, W> {
    fn reserve(
        turn: schedule::Turn<'a, 'd>,
        prior: bool,
    ) -> schedule::Capacity<Reserved<'a, 'd, batch::Capacity<'d, W>>> {
        let work = turn.app();
        let Some(slots) = work.remaining().checked_sub(usize::from(prior)) else {
            return schedule::Capacity::Exhausted;
        };
        let Some(capacity) = batch::Capacity::fit_spare(batch::Spare::from_usize(slots)) else {
            return schedule::Capacity::Exhausted;
        };
        Reserved::reserve(work, capacity, prior)
    }

    fn new(
        driver: &'a mut retained::Context<'c, 'owner, 'd>,
        turn: schedule::Turn<'a, 'd>,
        held: Reserved<'a, 'd, batch::Capacity<'d, W>>,
    ) -> Self {
        Self {
            driver,
            turn,
            reservation: held,
        }
    }

    pub(in crate::app::receive::ingress) fn with<R>(
        self,
        apply: impl for<'id> FnOnce(
            &'a mut retained::Context<'c, 'owner, 'd>,
            schedule::Turn<'a, 'd>,
            Bound<'id, batch::Capacity<'d, W>>,
        ) -> (R, mem::raw::Unused<'id>),
    ) -> R {
        let Self {
            driver,
            turn,
            reservation: held,
        } = self;
        held.with((driver, turn), |(driver, turn), bound| {
            apply(driver, turn, bound)
        })
    }
}

impl<'a, 'c, 'owner, 'd: 'owner, W: wire::Spec + 'd> From<Grant<'a, 'c, 'owner, 'd, W>>
    for (
        &'a mut retained::Context<'c, 'owner, 'd>,
        schedule::Turn<'a, 'd>,
    )
{
    fn from(work: Grant<'a, 'c, 'owner, 'd, W>) -> Self {
        work.with(|driver, turn, bound| ((driver, turn), bound.empty()))
    }
}

const _: () = assert!(
    size_of::<schedule::Capacity<Grant<'static, 'static, 'static, 'static, wire::Identity>>>()
        == size_of::<Option<Grant<'static, 'static, 'static, 'static, wire::Identity>>>()
);
