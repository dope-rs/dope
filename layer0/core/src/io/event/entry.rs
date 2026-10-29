use std::mem;

use crate::driver::{route, route::kind};

pub(super) struct Entry<'d, C: kind::Class, T> {
    target: route::Erased<'d, C>,
    value: T,
}

impl<'d, C: kind::Class, T> Entry<'d, C, T> {
    pub(super) const fn new(target: route::Erased<'d, C>, value: T) -> Self {
        const {
            assert!(mem::size_of::<Self>() == mem::size_of::<(route::Erased<'d, C>, T)>());
            assert!(mem::align_of::<Self>() == mem::align_of::<(route::Erased<'d, C>, T)>());
        }
        Self { target, value }
    }

    pub(super) const fn target(&self) -> route::Erased<'d, C> {
        self.target
    }

    pub(super) const fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn resolve(self, expected: route::Erased<'d, C>) -> Result<T, Self> {
        let Self { target, value } = self;
        if target.matches(expected) {
            Ok(value)
        } else {
            Err(Self { target, value })
        }
    }

    pub(super) fn into_parts(self) -> (route::Erased<'d, C>, T) {
        (self.target, self.value)
    }

    pub(super) const fn split(self) -> (route::Erased<'d, C>, T)
    where
        T: Copy,
    {
        (self.target, self.value)
    }
}
