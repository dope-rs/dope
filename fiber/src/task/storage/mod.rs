pub mod fixed;
mod slab;

use std::marker;

use o3::collections::slab::key;
pub use slab::Slab;

type Invariant<'d, T> = (fn(&'d ()) -> &'d (), *mut T);

pub struct Id<'d, Tag = ()> {
    parts: key::Parts,
    marker: marker::PhantomData<Invariant<'d, Tag>>,
}

impl<'d, Tag> Id<'d, Tag> {
    pub(crate) fn from_key(key: key::Handle<Tag>) -> Self {
        Self {
            parts: key.parts(),
            marker: marker::PhantomData,
        }
    }

    pub(crate) fn parts(&self) -> key::Parts {
        self.parts
    }

    pub(crate) fn raw_index(&self) -> u32 {
        self.parts.index()
    }

    pub fn index(&self) -> usize {
        self.raw_index() as usize
    }
}

pub struct RoutedTag<Owner, const DOMAIN: u8, const ROUTE: u16> {
    marker: marker::PhantomData<*mut Owner>,
}

pub struct RoutedId<'d, Owner, const DOMAIN: u8, State> {
    parts: key::Parts,
    route: u16,
    state: State,
    marker: marker::PhantomData<Invariant<'d, Owner>>,
}

impl<'d, Owner, const DOMAIN: u8, const ROUTE: u16> Id<'d, RoutedTag<Owner, DOMAIN, ROUTE>> {
    pub fn into_routed<State>(self, state: State) -> RoutedId<'d, Owner, DOMAIN, State> {
        RoutedId {
            parts: self.parts,
            route: ROUTE,
            state,
            marker: marker::PhantomData,
        }
    }
}

impl<'d, Owner, const DOMAIN: u8, State> RoutedId<'d, Owner, DOMAIN, State> {
    pub fn route(&self) -> u16 {
        self.route
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn into_typed<const ROUTE: u16>(
        self,
    ) -> Result<(Id<'d, RoutedTag<Owner, DOMAIN, ROUTE>>, State), Self> {
        if self.route != ROUTE {
            return Err(self);
        }
        Ok((
            Id {
                parts: self.parts,
                marker: marker::PhantomData,
            },
            self.state,
        ))
    }

    pub fn index(&self) -> usize {
        self.parts.index() as usize
    }
}
