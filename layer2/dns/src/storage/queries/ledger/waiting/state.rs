use std::{mem, time};

use o3::mem::{Lease, Return, lease::Open};

use crate::{
    lookup::error,
    query::answer::reply,
    storage::queries::{index, ledger, state, stream, tx},
    wire::cookies,
};

#[must_use = "a claimed waiting DNS request must be transitioned or rolled back"]
pub(in crate::storage::queries) struct State<'ledger, 's, const N: usize> {
    active: Lease<Active<'ledger, 's, N>, Open>,
    hard: time::Instant,
}

struct Active<'ledger, 's, const N: usize> {
    slot: &'ledger mut state::Row<'s, N>,
    reg: &'ledger mut ledger::Registry<'s>,
    probe: state::Attempt<'s>,
}

impl<'ledger, 's, const N: usize> State<'ledger, 's, N> {
    pub(in crate::storage::queries::ledger) fn new(
        slot: &'ledger mut state::Row<'s, N>,
        reg: &'ledger mut ledger::Registry<'s>,
        probe: state::Attempt<'s>,
        hard: time::Instant,
    ) -> Self {
        Self {
            active: Lease::open(Active { slot, reg, probe }, ()),
            hard,
        }
    }

    pub(in crate::storage::queries) fn apply(
        self,
        reply: reply::Reply<'_, N>,
        retry: state::Retry,
        now: time::Instant,
    ) {
        self.transition(|probe, hard, live, row| probe.apply(reply, retry, hard, now, live, row));
    }

    pub(in crate::storage::queries) fn deadline(&self) -> time::Instant {
        self.active.sink().probe.deadline()
    }

    pub(in crate::storage::queries) fn failed(
        self,
        retry: state::Retry,
        now: time::Instant,
        cause: error::Kind,
    ) {
        self.transition(|probe, hard, live, row| probe.retry(retry, hard, now, live, cause, row));
    }

    pub(in crate::storage::queries) fn stream(self) {
        let Active { slot, reg, probe } = self.take();
        probe.stream(&mut reg.live, slot);
    }

    pub(in crate::storage::queries) fn cache(
        &mut self,
        cookie: tx::Cookie,
        server: index::Server,
        value: cookies::Server,
    ) -> bool {
        self.active
            .parts_mut()
            .0
            .reg
            .ids
            .cache(cookie, server, value)
    }

    pub(in crate::storage::queries) fn cookie(self) {
        let Active { slot, reg, probe } = self.take();
        probe.cookie(&mut reg.live, slot);
    }

    fn transition(
        self,
        f: impl FnOnce(state::Attempt<'s>, time::Instant, &mut stream::Live, &mut state::Row<'s, N>),
    ) {
        let hard = self.hard;
        let Active { slot, reg, probe } = self.take();
        f(probe, hard, &mut reg.live, slot);
    }

    fn take(self) -> Active<'ledger, 's, N> {
        self.active.into_parts().0
    }
}

impl<const N: usize> Return for Active<'_, '_, N> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.slot.pending(self.probe);
    }
}

const _: () = {
    assert!(mem::size_of::<State<'static, 'static, 1>>() == 344);
    assert!(
        mem::size_of::<State<'static, 'static, 1>>()
            == mem::size_of::<(Active<'static, 'static, 1>, time::Instant)>()
    );
};
