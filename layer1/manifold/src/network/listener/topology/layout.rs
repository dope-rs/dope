use std::{marker, mem};

use demands::slots;
use dope_core::driver::{
    route::{Identity, KeyTag, table::conn},
    settings::{self, claim, demands, errors},
};

use crate::app::receive;

/// Startup-only driver-resource layout for one listener route.
pub struct Layout<ID: Identity, I> {
    connections: conn::Capacity,
    input: crate::Family<(I, ID)>,
}

impl<ID: Identity, I> Clone for Layout<ID, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<ID: Identity, I> Copy for Layout<ID, I> {}

impl<ID: Identity, I> PartialEq for Layout<ID, I> {
    fn eq(&self, other: &Self) -> bool {
        self.connections == other.connections
    }
}

impl<ID: Identity, I> Eq for Layout<ID, I> {}

impl<ID: Identity, I> Layout<ID, I>
where
    I: receive::Resources,
{
    pub const fn new(cap: conn::Capacity) -> Self {
        Self {
            connections: cap,
            input: marker::PhantomData,
        }
    }
}

impl<ID: Identity, I> settings::Resource for Layout<ID, I>
where
    I: receive::Resources,
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let slots = slots::Slots::from(self.connections);
        let accept = demands::Accept::from(self.connections);
        needs.include(
            claim::Claim::new(settings::Routes::singleton::<ID>())
                .files(demands::Files::accept(accept))
                .domains(I::RESIDENT_DOMAINS)
                .schedule(demands::Schedule::new(slots, slots)),
        )
    }
}

const _: () = assert!(
    mem::size_of::<Layout<KeyTag<0>, receive::Borrowed>>() == mem::size_of::<conn::Capacity>()
);
