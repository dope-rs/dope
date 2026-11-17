use std::{convert, pin};

use dope_core::driver::{receive::Stale, route::Identity};
use dope_net::{
    link::{
        event,
        pool::recv::{arrival, delivery, input, reserve::Choice},
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

    type Admission<'a, 'c, 'd>
        = Grant<'a, 'c, 'd, W>
    where
        'c: 'a,
        'd: 'a + 'c,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'w, 'd, ID: Identity + 'd, P, A>(
        mut policy: pin::Pin<&mut P>,
        data: A,
        work: Self::Admission<'a, 'c, 'd>,
    ) where
        'c: 'a,
        'd: 'a + 'c + 'w + 'p,
        W: 'w + 'd,
        P: ingress::Policy<'p, 'w, 'd, ID, Wire = W, Input = M>,
        M::Reserve: Choice<W, M::Ingress>,
        A: arrival::Arrival<'d, input::Receipt<W, M>, Observed = event::Data<'d>>,
    {
        work.with(|driver, turn, bound| {
            let mut backing = None;
            let (pool, mut context) = P::parts(policy.as_mut());
            let (entry, (next, unused)) = pool.recv().data(
                data,
                |key, state| P::receipt(&mut context, key, state, driver.region_token()),
                |plan| match plan {
                    event::Reservation::Ready(prepared) => {
                        let (next, unused) = prepared.dispatch(&mut backing, bound);
                        (Some(Ok(next)), unused)
                    }
                    event::Reservation::Control(next) => {
                        (Some(Ok(event::DispatchRecv::Control(next))), bound.empty())
                    }
                    event::Reservation::Parked(parked) => (Some(Err(parked)), bound.empty()),
                    event::Reservation::Stale(Stale) | event::Reservation::Drop => {
                        (None, bound.empty())
                    }
                },
            );
            if let Some(entry) = entry {
                match next {
                    Some(Ok(next)) => drop(flow::apply_done::<ID, P>(
                        &mut context, entry, next, turn, driver,
                    )),
                    Some(Err(parked)) => drop(ingress::apply_parked::<ID, P>(
                        &mut context, entry, parked, turn, driver,
                    )),
                    None => {}
                }
            }
            ((), unused)
        })
    }
}
