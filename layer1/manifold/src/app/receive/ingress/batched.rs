use std::{convert, pin};

use dope_core::driver::{receive::Stale, route::Identity};
use dope_net::{
    link::{
        event,
        pool::recv::{arrival, delivery, reserve::Choice},
    },
    wire::{self, receive},
};
use receive::batch::Items;

use crate::app::receive::{
    Batch,
    ingress::{self, Dispatch, Strategy, batch::Grant, dispatch::flow},
};

pub(crate) struct Batched;

impl<W, M> Strategy<W, M> for Batched
where
    W: wire::Spec,
    M: Batch + Dispatch<W, Strategy = Batched>,
    M::Kind: delivery::BorrowedData,
{
    type Value<'input, 'w, 'd>
        = Items<'input, W>
    where
        'd: 'w,
        'w: 'input,
        W: 'w + 'd;

    type Pending<'w, 'd>
        = convert::Infallible
    where
        'd: 'w,
        W: 'w + 'd;

    type Admission<'a, 'c, 'owner, 'd>
        = Grant<'a, 'c, 'owner, 'd, W>
    where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'owner, 'w, 'd, ID: Identity + 'd, P, A>(
        mut policy: pin::Pin<&mut P>,
        data: A,
        work: Self::Admission<'a, 'c, 'owner, 'd>,
    ) where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner + 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
    {
        work.with(|driver, turn, bound| {
            let mut backing = None;
            let control = 'admit: {
                let prep = match P::storage(policy.as_mut()).reserve().data(data) {
                    event::Reservation::Ready(prepared) => prepared,
                    event::Reservation::Control(next) => break 'admit Ok(next),
                    event::Reservation::Parked(parked) => break 'admit Err(parked),
                    event::Reservation::Stale(Stale) | event::Reservation::Drop => {
                        return ((), bound.empty());
                    }
                };
                let (next, unused) = prep.dispatch(&mut backing, bound);
                flow::apply_done(
                    policy.as_mut(),
                    next,
                    turn,
                    driver,
                    |policy, key, input, turn, driver| {
                        P::receive(policy, key, M::from_value(input), turn, driver)
                    },
                );
                return ((), unused);
            };
            let unused = bound.empty();
            match control {
                Ok(next) => ingress::apply_control(policy.as_mut(), next, turn, driver),
                Err(parked) => ingress::apply_parked(policy.as_mut(), parked, turn, driver),
            }
            ((), unused)
        })
    }
}
