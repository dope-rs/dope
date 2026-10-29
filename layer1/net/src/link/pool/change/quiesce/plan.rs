//! Backend-quiescent connection teardown.
use dope_core::driver::{life::finish, route::Identity};
use o3::base::cell::region;

use crate::{
    link::{
        egress::{self, reserve::demand},
        pool::{
            Key,
            change::{
                self,
                quiesce::{Entry, Strategy},
            },
            recv::{input, storage::tail::Slot},
        },
    },
    wire,
};

pub(in crate::link::pool::change) struct Plan<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode,
> where
    'd: 'w,
{
    pub(in crate::link::pool::change) prepared:
        &'a mut change::raw::Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Plan<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    'd: 'w,
{
    pub(in crate::link::pool::change) fn new(
        prepared: &'a mut change::raw::Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Self {
        Self { prepared }
    }

    pub(in crate::link::pool::change) fn run(
        &mut self,
        finish: &mut finish::Finish<'_, 'd>,
        mut free: impl FnMut(B, &mut region::Token<region::Identity<'d, 1>>),
    ) where
        M::Ingress: Strategy<W>,
    {
        self.retire(finish, |egress, key, finish| {
            egress.finish(key, finish, &mut free)
        });
    }

    pub(in crate::link::pool::change) fn keyed(
        &mut self,
        finish: &mut finish::Finish<'_, 'd>,
        mut free: impl FnMut(Key<'d, ID>, B),
    ) where
        M::Ingress: Strategy<W>,
    {
        self.retire(finish, |egress, key, finish| {
            egress.finish(key, finish, |payload, _| free(key, payload))
        });
    }

    fn retire(
        &mut self,
        finish: &mut finish::Finish<'_, 'd>,
        mut free: impl FnMut(
            &mut egress::Storage<'p, 'd, ID, B, IOV, D>,
            Key<'d, ID>,
            &mut finish::Finish<'_, 'd>,
        ),
    ) where
        M::Ingress: Strategy<W>,
    {
        if let Some(storage) = <M::Ingress as Strategy<W>>::credits(self) {
            finish.credit().detach(storage);
        }
        if self.prepared.slab.view().is_empty() {
            return;
        }
        for index in self.prepared.slab.view().capacity() {
            let Some((entry, storage)) = Slot::at::<M::Ingress>(&mut self.prepared.slab, index)
            else {
                continue;
            };
            if let Some(handle) = self.prepared.pending.handle(entry.get().key) {
                handle.wait_send(false);
            }
            let plan = Entry::<ID, W, S, M, B, IOV, D> {
                entry,
                egress: &mut self.prepared.egress,
                recv: storage,
            };
            <M::Ingress as Strategy<W>>::retire(
                plan,
                &mut self.prepared.runtime,
                finish,
                &mut free,
            );
        }
    }
}
