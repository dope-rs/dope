use std::{convert, pin};

use dope_core::driver::route::Identity;
use dope_net::{
    link::{
        event,
        pool::recv::{arrival, delivery, input, reserve::Choice},
    },
    wire::{receive, receive::Chunk},
};

use crate::app::receive::ingress::{self, Dispatch, Strategy, dispatch::flow};

pub(crate) struct Retain;

impl<W, M> Strategy<W, M> for Retain
where
    W: input::Execution,
    M: Dispatch<W, Strategy = Retain, Kind = input::Retained>,
    <W::Receive as receive::Policy<W>>::Credit: delivery::Ownership<W>,
{
    type Value<'input, 'w, 'd>
        = Chunk<'w, 'd, W>
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
        = ingress::Single<'a, 'c, 'owner, 'd>
    where
        'c: 'a,
        'owner: 'a,
        'd: 'a + 'c + 'owner,
        W: 'd;

    fn drive_data<'p, 'a, 'c, 'owner, 'w, 'd, ID: Identity + 'd, P, A>(
        policy: pin::Pin<&mut P>,
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
        work.drive(
            policy,
            data,
            |prep| prep.retained(),
            |policy, next, turn, driver| {
                flow::apply_done(
                    policy,
                    next,
                    turn,
                    driver,
                    |policy, key, input, turn, driver| {
                        P::receive(policy, key, M::from_value(input), turn, driver)
                    },
                )
            },
        )
    }
}
