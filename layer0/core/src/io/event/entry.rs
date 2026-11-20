use std::mem;

use crate::driver::{route, route::kind};

pub struct Entry<'d, C: kind::Class, T, S: route::Selection = route::Route> {
    target: route::Erased<'d, C, S>,
    value: T,
}

impl<'d, C: kind::Class, T, S: route::Selection> Entry<'d, C, T, S> {
    pub(in crate::io) const fn new(target: route::Erased<'d, C, S>, value: T) -> Self {
        const {
            assert!(mem::size_of::<Self>() == mem::size_of::<(route::Erased<'d, C>, T)>());
            assert!(mem::align_of::<Self>() == mem::align_of::<(route::Erased<'d, C>, T)>());
        }
        Self { target, value }
    }

    pub const fn token(&self) -> route::Erased<'d, C, S> {
        self.target
    }

    pub(super) fn in_scope<N: route::Selection>(self, scope: N) -> Entry<'d, C, T, N> {
        Entry::new(self.target.in_scope(scope), self.value)
    }

    pub(super) fn erase_route(self) -> Entry<'d, C, T> {
        Entry::new(self.target.erase_route(), self.value)
    }

    pub(super) const fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn resolve(self, expected: route::Erased<'d, C>) -> Result<T, Self> {
        let Self { target, value } = self;
        if target.matches_flight(expected) {
            Ok(value)
        } else {
            Err(Self { target, value })
        }
    }

    pub(super) fn split(self) -> (route::Erased<'d, C, S>, T) {
        let Self { target, value } = self;
        (target, value)
    }
}

impl<'d, C: kind::Class, T: Copy, S: route::Selection> Entry<'d, C, T, S> {
    pub const fn outcome(&self) -> T {
        self.value
    }

    pub const fn into_parts(self) -> (route::Erased<'d, C, S>, T) {
        (self.target, self.value)
    }
}
