use std::marker;

use dope_core::driver::route::Identity;
use receive::{
    mode,
    reserve::{Guard, Immediate, Kind},
};

use crate::wire::{self, receive};

/// Zero-storage reservation for receive policies which never backpressure.
pub struct Direct;

impl<W: wire::Spec> Kind<W> for Direct {
    type Wait = mode::Disabled;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
        = Guard<ID, W>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
}

impl<W: wire::Spec> Immediate<W> for Direct {
    fn reserve<'a, 'w, 'd, ID: Identity + 'w>(
        _: &'a mut W::Connection<'w, ID>,
        _: &'a mut W::Runtime<'w, 'd, ID>,
    ) -> Self::Transaction<'a, 'w, 'd, ID>
    where
        W: 'w,
        'd: 'w,
        'w: 'a,
    {
        Direct::reserve::<ID, W>()
    }
}

impl Direct {
    pub(crate) const fn reserve<ID: Identity, W: wire::Spec>() -> Guard<ID, W> {
        Guard(marker::PhantomData)
    }
}

const _: () = assert!(size_of::<Direct>() == 0);
