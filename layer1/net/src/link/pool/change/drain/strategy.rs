use std::convert::Infallible;

use dope_core::driver::{receive::credit, route::Identity};
use o3::base::Call;

use crate::{
    link::{
        duplex,
        egress::reserve::demand,
        pool::{
            change::{
                close::{Approval, Commit, Decision, Deferred, Failed, Owner, Resume},
                drain::{Branch, Plan, plan::Step},
            },
            recv::{input, storage, storage::queue::Ops as _},
        },
        slot::recv::Capture,
    },
    wire::{
        self,
        receive::{Policy, mode, reserve},
    },
};

pub(in crate::link::pool::change) trait Strategy<W: wire::Spec>:
    storage::raw::Mode
{
    fn returned<'w, 'd: 'w, ID: Identity + 'w>(
        _: credit::Wake,
    ) -> Option<impl for<'a> Call<&'a mut W::Runtime<'w, 'd, ID>>>
    where
        W: 'w + 'd,
    {
        None::<Infallible>
    }

    fn credit<'s, 'a, 'd: 'a>(_: &'s Self::Bound<'a, 'd>) -> Option<credit::Slot<'s, 'd>> {
        None
    }

    fn cancel<'w, 'd: 'w, ID: Identity + 'w>(
        _: &Self::Bound<'_, 'd>,
    ) -> Option<impl for<'a> Call<(&'a mut W::Runtime<'w, 'd, ID>, credit::Id<'d, ID>)>>
    where
        W: 'w + 'd,
    {
        None::<Infallible>
    }

    fn close<
        'a,
        'm,
        'r,
        'o,
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
        mut plan: Plan<'a, 'm, 'r, 'o, 'p, 'w, 'd, ID, W, S, M, B, IOV, D, C, X, O, K, H>,
    ) -> Branch<'d, ID, R, B>
    where
        'd: 'w + 'o,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = Self>,
        D: demand::Mode,
        <W::Receive as Policy<W>>::Eof: Capture<W>,
        H: for<'x, 'c, 'i, 'v> FnMut(
            Failed<'x, 'c, 'i, 'v, 'r, 'o, 'p, 'w, 'd, ID, W, S, B, IOV, D, C, X>,
        ),
        O: for<'x, 'c, 'f, 's, 'v> FnMut(
            Owner<'x, 'c, 'f, 's, 'v, 'r, 'o, 'w, 'd, ID, W, S, C, X>,
        ) -> Decision<'d, ID>,
        K: for<'x, 'c, 'v> FnMut(Commit<'x, 'c, 'v, 'r, 'o, 'd, C, X, S>) -> Approval<S, R>,
    {
        let wake = Self::credit(&plan.storage.recv.storage).map(credit::Slot::take);
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
                if let Some(returned) = Self::returned(wake) {
                    returned.call(plan.storage.runtime);
                }
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
        let active = Self::credit(&plan.storage.recv.storage).is_some_and(credit::Slot::is_active);
        if active || plan.storage.recv.storage.has_queued() {
            Branch::Pending(Deferred::Ingress)
        } else {
            plan.retire()
        }
    }
}

impl<W: wire::Spec> Strategy<W> for mode::Disabled {}

impl<W: wire::Spec> Strategy<W> for mode::Parking {}

impl<W> Strategy<W> for mode::Enabled
where
    W: wire::Spec,
    <W::Receive as Policy<W>>::Reserve: reserve::Immediate<W>,
{
    fn credit<'s, 'a, 'd: 'a>(storage: &'s Self::Bound<'a, 'd>) -> Option<credit::Slot<'s, 'd>> {
        Some(storage.get())
    }
}

impl<W, R> Strategy<W> for mode::Waiting<R>
where
    W: wire::Spec,
    R: reserve::Try<W>,
    W::Receive: Policy<W, Reserve = reserve::Waiting<R>>,
{
    fn cancel<'w, 'd: 'w, ID: Identity + 'w>(
        storage: &Self::Bound<'_, 'd>,
    ) -> Option<impl for<'a> Call<(&'a mut W::Runtime<'w, 'd, ID>, credit::Id<'d, ID>)>>
    where
        W: 'w + 'd,
    {
        storage.get().is_held().then_some(
            |(runtime, target): (&mut W::Runtime<'w, 'd, ID>, credit::Id<'d, ID>)| {
                R::cancel(runtime, target);
            },
        )
    }

    fn returned<'w, 'd: 'w, ID: Identity + 'w>(
        wake: credit::Wake,
    ) -> Option<impl for<'a> Call<&'a mut W::Runtime<'w, 'd, ID>>>
    where
        W: 'w + 'd,
    {
        match wake {
            credit::Wake::Returned => Some(R::release_recv::<ID>),
            credit::Wake::Retry => None,
        }
    }

    fn credit<'s, 'a, 'd: 'a>(storage: &'s Self::Bound<'a, 'd>) -> Option<credit::Slot<'s, 'd>> {
        Some(storage.get())
    }
}
