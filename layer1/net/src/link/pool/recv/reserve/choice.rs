use dope_core::driver::route::Identity;

use crate::{
    link::{
        event,
        pool::recv::{
            input,
            reserve::plan::{Plan, Queued},
        },
    },
    wire::{
        self,
        receive::{
            self,
            mode::{self, Flag},
            reserve::{Immediate, Try, Waiting},
        },
    },
};

/// Static leaf for each realizable reservation and ingress pair.
pub trait Choice<W: wire::Spec, P: Flag> {
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, S, M>(
        plan: Plan<'a, 'w, 'd, ID, W, S, M>,
    ) -> super::Reservation<'a, 'w, 'd, ID, W, M>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = P>,
        input::Receipt<W, M>: 'a;
}

impl<W, R> Choice<W, mode::Disabled> for R
where
    W: wire::Spec,
    R: Immediate<W>,
    W::Receive: receive::Policy<W, Reserve = R>,
{
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, S, M>(
        plan: Plan<'a, 'w, 'd, ID, W, S, M>,
    ) -> super::Reservation<'a, 'w, 'd, ID, W, M>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Disabled>,
        input::Receipt<W, M>: 'a,
    {
        plan.immediate(mode::Direct::NEW)
    }
}

impl<W, R> Choice<W, mode::Parking> for R
where
    W: wire::Spec,
    R: Immediate<W>,
    W::Receive: receive::Policy<W, Reserve = R>,
{
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, S, M>(
        plan: Plan<'a, 'w, 'd, ID, W, S, M>,
    ) -> super::Reservation<'a, 'w, 'd, ID, W, M>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Parking>,
        input::Receipt<W, M>: 'a,
    {
        match plan.queue() {
            Queued::Parked(parked) => event::Reservation::Parked(parked),
            Queued::Ready(plan) => plan.immediate(mode::Direct::NEW),
        }
    }
}

impl<W, R> Choice<W, mode::Enabled> for R
where
    W: wire::Spec,
    R: Immediate<W>,
    W::Receive: receive::Policy<W, Reserve = R>,
{
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, S, M>(
        plan: Plan<'a, 'w, 'd, ID, W, S, M>,
    ) -> super::Reservation<'a, 'w, 'd, ID, W, M>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Enabled>,
        input::Receipt<W, M>: 'a,
    {
        match plan.queue() {
            Queued::Parked(parked) => event::Reservation::Parked(parked),
            Queued::Ready(plan) => {
                let credit = plan.credit();
                plan.immediate(credit)
            }
        }
    }
}

impl<W, R> Choice<W, mode::Waiting<R>> for Waiting<R>
where
    W: wire::Spec,
    R: Try<W>,
    W::Receive: receive::Policy<W, Reserve = Waiting<R>>,
{
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, S, M>(
        plan: Plan<'a, 'w, 'd, ID, W, S, M>,
    ) -> super::Reservation<'a, 'w, 'd, ID, W, M>
    where
        'd: 'w,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Waiting<R>>,
        input::Receipt<W, M>: 'a,
    {
        match plan.queue() {
            Queued::Parked(parked) => event::Reservation::Parked(parked),
            Queued::Ready(plan) => plan.try_reserve(),
        }
    }
}
