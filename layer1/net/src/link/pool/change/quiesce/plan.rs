//! Backend-quiescent connection teardown.
use dope_core::driver::{
    life::finish,
    receive::{Absent, Registration, credit},
    route::Identity,
};
use o3::base::{Call as _, cell::region};

use crate::{
    link::{
        egress::{self, reserve::demand},
        event,
        pool::{
            Key,
            change::{self, drain, quiesce::Strategy},
            recv::{
                input,
                storage::{queue::Ops as _, tail, tail::Slot},
            },
            send::Finish,
        },
    },
    wire::{self, send::Store as _},
};

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    change::raw::Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    'd: 'w,
{
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
        if let Some(storage) = <M::Ingress as Strategy<W>>::credits(&self.slab) {
            finish.credit().detach(storage);
        }
        if self.slab.view().is_empty() {
            return;
        }
        for index in self.slab.view().capacity() {
            let released = {
                let Some((entry, mut recv)) = Slot::at::<M::Ingress>(&mut self.slab, index) else {
                    continue;
                };
                if let Some(handle) = self.pending.handle(entry.get().key) {
                    handle.wait_send(false);
                }
                let target = credit::Id::new(entry.get().key.target().dispatch());
                if let Some(cancel) = <M::Ingress as drain::Strategy<W>>::cancel(&recv.storage) {
                    cancel.call((&mut self.runtime, target));
                }
                while let Some(front) = recv.storage.prepare_pop() {
                    match front.pop().classify() {
                        event::Kind::Data(..) => {}
                        event::Kind::Control(control) => finish.reclaim(control.retire()),
                    }
                }
                let mut entry = tail::Store::clear(entry);
                let target = entry.get().engine.ready_handle().target();
                match Registration::try_acquire(target) {
                    Ok(reg) => reg.retire(),
                    Err(Absent) => entry.get_mut().engine.operations.recv.exhaustion().cancel(),
                }
                free(&mut self.egress, entry.get().key, finish);
                entry.remove().send.release()
            };
            if let Some(release) = <M::Ingress as Finish<W>>::release::<ID>(released) {
                release.call(&mut self.runtime);
            }
        }
    }
}
