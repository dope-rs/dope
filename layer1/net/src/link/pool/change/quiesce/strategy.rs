use dope_core::driver::{life::finish, receive::credit, route::Identity};
use o3::base::Call as _;

use crate::{
    link::{
        egress::{self, reserve::demand},
        pool::{
            self,
            change::{
                drain,
                quiesce::{Entry, Plan},
            },
            recv::{input, storage},
            send::Finish,
        },
    },
    wire::{
        self,
        receive::{Policy, mode, reserve},
    },
};

pub(in crate::link::pool::change) trait Strategy<W: wire::Spec>:
    storage::raw::Mode + drain::Strategy<W> + Finish<W>
{
    fn credits<'a, 'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        _: &'a Plan<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Option<&'a credit::Storage<'d>>
    where
        Self: 'a,
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        None
    }

    fn retire<'a, 'p, 'w, 'd, ID: Identity + 'w, S, M, B, const IOV: usize, D, F>(
        plan: Entry<'a, 'p, 'w, 'd, ID, W, S, M, B, IOV, D>,
        runtime: &mut W::Runtime<'w, 'd, ID>,
        finish: &mut finish::Finish<'_, 'd>,
        free: F,
    ) where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
        F: FnOnce(
            &mut egress::Storage<'p, 'd, ID, B, IOV, D>,
            pool::Key<'d, ID>,
            &mut finish::Finish<'_, 'd>,
        ),
    {
        let target = credit::Id::new(plan.entry.get().key.target().dispatch());
        if let Some(cancel) = <Self as drain::Strategy<W>>::cancel(&plan.recv.storage) {
            cancel.call((runtime, target));
        }
        if let Some(release) = Self::release::<ID>(plan.retire(finish, free)) {
            release.call(runtime);
        }
    }
}

impl<W> Strategy<W> for mode::Disabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W> Strategy<W> for mode::Parking
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
}

impl<W> Strategy<W> for mode::Enabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
    fn credits<'a, 'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        plan: &'a Plan<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Option<&'a credit::Storage<'d>>
    where
        Self: 'a,
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        Some(plan.prepared.slab.credit())
    }
}

impl<W, R> Strategy<W> for mode::Waiting<R>
where
    W: wire::Spec,
    R: reserve::Try<W>,
    W::Receive: Policy<W, Reserve = reserve::Waiting<R>>,
{
    fn credits<'a, 'p, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D>(
        plan: &'a Plan<'_, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Option<&'a credit::Storage<'d>>
    where
        Self: 'a,
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
    {
        Some(plan.prepared.slab.credit())
    }
}
