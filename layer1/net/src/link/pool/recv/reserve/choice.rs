use dope_core::driver::route::Identity;

use crate::{
    link::{
        egress::reserve::demand,
        event,
        pool::{
            self,
            recv::{
                arrival, delivery, input,
                reserve::plan::{Plan, Queued},
            },
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
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        done: A,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = P>,
        D: demand::Mode,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>;
}

impl<W, R> Choice<W, mode::Disabled> for R
where
    W: wire::Spec,
    R: Immediate<W>,
    W::Receive: receive::Policy<W, Reserve = R>,
{
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        done: A,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Disabled>,
        D: demand::Mode,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        let plan = match Plan::observe(pool, done) {
            Ok(plan) => plan,
            Err(err) => return err,
        };
        plan.immediate(mode::Direct::NEW)
    }
}

impl<W, R> Choice<W, mode::Parking> for R
where
    W: wire::Spec,
    R: Immediate<W>,
    W::Receive: receive::Policy<W, Reserve = R>,
{
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        done: A,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Parking>,
        D: demand::Mode,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        let plan = match Plan::observe(pool, done) {
            Ok(plan) => plan,
            Err(err) => return err,
        };
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
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        done: A,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Enabled>,
        D: demand::Mode,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        let plan = match Plan::observe(pool, done) {
            Ok(plan) => plan,
            Err(err) => return err,
        };
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
    fn reserve<'a, 'w, 'd, ID: Identity + 'w, T, S, M, B, const IOV: usize, D, A>(
        pool: &'a mut pool::Connections<'_, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        done: A,
    ) -> event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, M::Kind>>
    where
        'd: 'w,
        T: crate::Transport,
        W: 'w + 'd,
        M: input::Mode<W, Ingress = mode::Waiting<R>>,
        D: demand::Mode,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        let plan = match Plan::observe(pool, done) {
            Ok(plan) => plan,
            Err(err) => return err,
        };
        match plan.queue() {
            Queued::Parked(parked) => event::Reservation::Parked(parked),
            Queued::Ready(plan) => plan.try_reserve(),
        }
    }
}
