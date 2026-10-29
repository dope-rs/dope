mod choice;
mod plan;

pub use choice::Choice;
use dope_core::driver::route::Identity;

use crate::{
    link::{
        egress::reserve::demand,
        event,
        pool::{
            self,
            recv::{arrival, delivery, input},
        },
    },
    wire,
};

pub struct Reserve<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Reserve<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub(in crate::link::pool) fn new(
        pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    ) -> Self {
        Self { pool }
    }

    /// Reserves the statically selected ingress policy for one observed data
    /// completion.
    pub fn data<A>(self, done: A) -> Reservation<'a, 'w, 'd, ID, W, M>
    where
        A: arrival::Arrival<'d, Observed = event::Data<'d>>,
        M::Reserve: Choice<W, M::Ingress>,
    {
        <M::Reserve as Choice<W, M::Ingress>>::reserve(self.pool, done)
    }
}

type Reservation<'a, 'w, 'd, ID, W, M> =
    event::Reservation<'d, ID, delivery::Data<'a, 'w, 'd, ID, W, <M as input::Mode<W>>::Kind>>;
