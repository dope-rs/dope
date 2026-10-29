use dope_core::driver::route::Identity;
use receive::{
    mode,
    reserve::{Immediate, Kind, Transaction},
};

use crate::wire::{self, receive};

/// Immediate reservation carrying the connection-owner proof.
pub struct Borrowed;

impl<W: wire::Spec> Kind<W> for Borrowed {
    type Wait = mode::Disabled;
    type Transaction<'a, 'w, 'd, ID: Identity + 'w>
        = Transaction<'a, 'w, ID, W>
    where
        W: 'w,
        'd: 'w,
        'w: 'a;
}

impl<W: wire::Spec> Immediate<W> for Borrowed {
    fn reserve<'a, 'w, 'd, ID: Identity + 'w>(
        wire: &'a mut W::Connection<'w, ID>,
        _: &'a mut W::Runtime<'w, 'd, ID>,
    ) -> Self::Transaction<'a, 'w, 'd, ID>
    where
        W: 'w,
        'd: 'w,
        'w: 'a,
    {
        Transaction::new(wire)
    }
}

const _: () = assert!(size_of::<Borrowed>() == 0);
