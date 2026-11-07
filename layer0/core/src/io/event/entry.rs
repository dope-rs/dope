use std::mem;

use crate::driver::{route, route::kind};

pub struct Entry<'d, C: kind::Class, T> {
    target: route::Erased<'d, C>,
    value: T,
}

impl<'d, C: kind::Class, T> Entry<'d, C, T> {
    pub(in crate::io) const fn new(target: route::Erased<'d, C>, value: T) -> Self {
        const {
            assert!(mem::size_of::<Self>() == mem::size_of::<(route::Erased<'d, C>, T)>());
            assert!(mem::align_of::<Self>() == mem::align_of::<(route::Erased<'d, C>, T)>());
        }
        Self { target, value }
    }

    pub const fn token(&self) -> route::Erased<'d, C> {
        self.target
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

    pub(super) fn split(self) -> (route::Erased<'d, C>, T) {
        let Self { target, value } = self;
        (target, value)
    }
}

impl<'d, C: kind::Class, T: Copy> Entry<'d, C, T> {
    pub const fn outcome(&self) -> T {
        self.value
    }

    pub const fn into_parts(self) -> (route::Erased<'d, C>, T) {
        (self.target, self.value)
    }
}
