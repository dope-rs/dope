use dope_core::driver;
use o3::{alloc, base::cell::region, buffer::resident::bound};

use crate::link::egress::{
    self, create,
    data::position,
    load::credit,
    metadata::{self, arena},
    reserve::{self, demand},
};

pub(in crate::link::egress) mod store;
mod vacant;

pub(in crate::link::egress) use vacant::{Bound, Vacant};

pub(in crate::link::egress) struct Pool<
    'p,
    'd,
    T,
    M = metadata::Local,
    D: demand::Mode = demand::Fallible,
    P: position::State = (),
> {
    pub(in crate::link::egress) store: store::Slab<'p, 'd, T, P>,
    pub(in crate::link::egress) credit: credit::Pool<'p, 'd, M>,
    config: egress::Config,
    mode: D,
}

impl<'p, 'd, T> Pool<'p, 'd, T, metadata::Local, demand::Fallible> {
    pub(in crate::link::egress) fn try_new(
        token: &region::Token<region::Identity<'d>>,
        slots: &'p arena::Store<'d>,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            store: store::Slab::try_new(token, slots.config.entries())?,
            credit: credit::Pool::new(slots),
            config: slots.config,
            mode: demand::Fallible,
        })
    }
}

impl<'p, 'd, T, D: demand::Mode, P: position::State> Pool<'p, 'd, T, credit::Ready<'p, 'd>, D, P> {
    pub(in crate::link::egress) fn try_driver(
        token: &region::Token<region::Identity<'d>>,
        mode: D,
        slots: &'p arena::Store<'d>,
        owner: driver::Reference<'d>,
    ) -> Result<Self, create::Error> {
        Ok(Self {
            store: store::Slab::try_new(token, slots.config.entries())
                .map_err(create::Error::Allocation)?,
            credit: credit::Pool::try_driver(token, slots, owner)?,
            config: slots.config,
            mode,
        })
    }
}

impl<'p, T, M, D: demand::Mode, P: position::State> Pool<'p, '_, T, M, D, P> {
    pub(in crate::link::egress) const fn config(&self) -> egress::Config {
        self.config
    }
}

impl<'p, T, M, G: bound::Bound, const PREFIX: usize, P: position::State>
    Pool<'p, '_, T, M, demand::Guaranteed<G, PREFIX>, P>
{
    pub(in crate::link::egress) const fn bounds(&self) -> Option<reserve::Bounds> {
        self.mode.bounds()
    }
}
