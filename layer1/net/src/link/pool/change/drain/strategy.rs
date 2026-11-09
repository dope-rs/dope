use dope_core::driver::{receive::credit, route::Identity};

use crate::{
    link::{
        duplex,
        egress::reserve::demand,
        pool::{
            change::{
                close::{Approval, Commit, Decision, Failed, Owner, Resume},
                drain::{Branch, Plan, plan::Step},
            },
            recv::{self, input, storage, storage::queue::Ops as _, storage::tail},
        },
        slot::recv::Capture,
    },
    wire::{
        self,
        receive::{Policy, mode, reserve},
    },
};

pub(in crate::link::pool::change) trait Strategy<W: wire::Spec>:
    Sized + storage::raw::Mode
{
    fn credits<'a, 'd, ID: Identity, T, S, R>(
        _: &'a tail::Table<T, ID, S, recv::State<'d, ID, Self, R>>,
    ) -> Option<&'a credit::Storage<'d>> {
        None
    }

    fn returned<'w, 'd: 'w, ID: Identity + 'w>(_: credit::Wake, _: &mut W::Runtime<'w, 'd, ID>)
    where
        W: 'w + 'd,
    {
    }

    fn credit<'s, 'a, 'd: 'a, R: 'a>(
        _: &'s Self::Bound<'a, 'd, R>,
    ) -> Option<credit::Slot<'s, 'd>> {
        None
    }

    fn cancel<'w, 'd: 'w, ID: Identity + 'w, C>(
        _: &Self::Bound<'_, 'd, C>,
        _: &mut W::Runtime<'w, 'd, ID>,
        _: credit::Id<'d, ID>,
    ) where
        W: 'w + 'd,
    {
    }

    fn close<
        'a,
        'm,
        'r,
        'p,
        'w,
        'd,
        ID: Identity + 'w,
        S,
        M,
        B,
        const IOV: usize,
        D,
        R,
        C,
        X,
        O,
        K,
        H,
    >(
        mut plan: Plan<'a, 'm, 'r, 'p, 'w, 'd, ID, W, S, M, B, IOV, D, C, X, O, K, H>,
    ) -> Branch<'d, ID, R, B>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        input::Receipt<W, M>: 'a,
        D: demand::Mode,
        <W::Receive as Policy<W>>::Eof: Capture<W>,
        H: for<'x, 'c, 'i, 'v> FnMut(
            Failed<'x, 'c, 'i, 'v, 'r, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
        ),
        O: for<'x, 'c, 'f, 's, 'v> FnMut(
            Owner<'x, 'c, 'f, 's, 'v, 'r, 'w, 'd, ID, W, S, C, X>,
        ) -> Decision<'d, ID>,
        K: for<'x, 'c, 'v> FnMut(Commit<'x, 'c, 'v, 'r, 'd, C, X, S>) -> Approval<S, R>,
    {
        let wake = Self::credit::<input::Receipt<W, M>>(&plan.storage.recv.storage)
            .map(credit::Slot::take);
        match wake {
            Some(credit::Take::Ready(wake)) => {
                let rearm = plan
                    .storage
                    .entry
                    .get_mut()
                    .engine
                    .operations
                    .recv
                    .resume(true)
                    .is_some_and(duplex::Rearm::is_needed);
                Self::returned(wake, plan.storage.runtime);
                if rearm {
                    plan.storage.recv.rearm.queue();
                }
                return Branch::Resume(Resume::new(plan.storage.entry.get()));
            }
            Some(credit::Take::Idle) | None => {}
        }
        match plan.pop() {
            Step::Done(branch) => return branch,
            Step::Continue => {}
        }
        match plan.advance() {
            Step::Done(branch) => return branch,
            Step::Continue => {}
        }
        let active = Self::credit::<input::Receipt<W, M>>(&plan.storage.recv.storage)
            .is_some_and(credit::Slot::is_active);
        if active || plan.storage.recv.storage.has_queued() {
            Branch::Pending
        } else {
            plan.retire()
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
    fn credits<'a, 'd, ID: Identity, T, S, R>(
        slab: &'a tail::Table<T, ID, S, recv::State<'d, ID, Self, R>>,
    ) -> Option<&'a credit::Storage<'d>> {
        Some(slab.credit())
    }

    fn credit<'s, 'a, 'd: 'a, R: 'a>(
        storage: &'s Self::Bound<'a, 'd, R>,
    ) -> Option<credit::Slot<'s, 'd>> {
        Some(storage.get())
    }
}

impl<W, R> Strategy<W> for mode::Waiting<R>
where
    W: wire::Spec,
    R: reserve::Try<W>,
    W::Receive: Policy<W, Reserve = reserve::Waiting<R>>,
{
    fn credits<'a, 'd, ID: Identity, T, S, C>(
        slab: &'a tail::Table<T, ID, S, recv::State<'d, ID, Self, C>>,
    ) -> Option<&'a credit::Storage<'d>> {
        Some(slab.credit())
    }

    fn cancel<'w, 'd: 'w, ID: Identity + 'w, C>(
        storage: &Self::Bound<'_, 'd, C>,
        runtime: &mut W::Runtime<'w, 'd, ID>,
        target: credit::Id<'d, ID>,
    ) where
        W: 'w + 'd,
    {
        if storage.get().is_held() {
            R::cancel(runtime, target);
        }
    }

    fn returned<'w, 'd: 'w, ID: Identity + 'w>(
        wake: credit::Wake,
        runtime: &mut W::Runtime<'w, 'd, ID>,
    ) where
        W: 'w + 'd,
    {
        if matches!(wake, credit::Wake::Returned) {
            R::release_recv(runtime);
        }
    }

    fn credit<'s, 'a, 'd: 'a, C: 'a>(
        storage: &'s Self::Bound<'a, 'd, C>,
    ) -> Option<credit::Slot<'s, 'd>> {
        Some(storage.get())
    }
}
