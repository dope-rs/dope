use dope_core::driver::{
    life::finish,
    receive::{Absent, Registration},
    route::Identity,
};

use crate::{
    link::{
        egress::{self, reserve::demand},
        event,
        pool::{
            self,
            recv::{
                self, input,
                storage::{queue::Ops, tail, tail::Slot},
            },
        },
        slot::types,
    },
    wire::{
        self,
        send::{self, Store as _},
    },
};

pub(in crate::link::pool::change) struct Entry<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode,
> where
    'd: 'w,
{
    pub(in crate::link::pool::change) entry:
        Slot<'a, types::Connection<'w, 'd, ID, W, S>, ID, input::Tail<'w, 'd, W, M>>,
    pub(in crate::link::pool::change) egress: &'a mut egress::Storage<'p, 'd, ID, B, IOV, D>,
    pub(in crate::link::pool::change) recv: recv::Borrow<'a, 'd, ID, M::Ingress>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, W, S, M, B, const IOV: usize, D>
    Entry<'a, 'p, 'w, 'd, ID, W, S, M, B, IOV, D>
where
    'd: 'w,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub(in crate::link::pool::change) fn retire(
        mut self,
        finish: &mut finish::Finish<'_, 'd>,
        free: impl FnOnce(
            &mut egress::Storage<'p, 'd, ID, B, IOV, D>,
            pool::Key<'d, ID>,
            &mut finish::Finish<'_, 'd>,
        ),
    ) -> send::Availability {
        while let Some(front) = self.recv.storage.prepare_pop() {
            match front.pop().classify() {
                event::Kind::Data(..) => {}
                event::Kind::Control(control) => finish.reclaim(control.retire()),
            }
        }
        let mut entry = tail::Store::clear(self.entry);
        let target = entry.get().engine.ready_handle().target();
        match Registration::try_acquire(target) {
            Ok(reg) => reg.retire(),
            Err(Absent) => entry.get_mut().engine.operations.recv.exhaustion().cancel(),
        }
        free(self.egress, entry.get().key, finish);
        entry.remove().send.release()
    }
}
