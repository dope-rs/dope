use std::mem;

use o3::depot::array;

use crate::config;

mod branch;
mod lane;
mod layout;

pub(in crate::storage) use branch::Branch;
pub(crate) use branch::Retirement;
pub(in crate::storage) use lane::Lane;
pub(in crate::storage) use layout::Layout;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Server(array::Index<{ config::MAX_SERVERS }>);

impl Server {
    pub(in crate::storage::queries) const ALL: [Self; config::MAX_SERVERS] = [
        Self(array::Index::at::<0>()),
        Self(array::Index::at::<1>()),
        Self(array::Index::at::<2>()),
        Self(array::Index::at::<3>()),
    ];

    pub(crate) fn get<T>(self, values: &[T; config::MAX_SERVERS]) -> &T {
        self.0.get(values)
    }

    pub(crate) fn get_mut<T>(self, values: &mut [T; config::MAX_SERVERS]) -> &mut T {
        self.0.get_mut(values)
    }
}

impl From<array::Vacancy<{ config::MAX_SERVERS }>> for Server {
    fn from(slot: array::Vacancy<{ config::MAX_SERVERS }>) -> Self {
        Self(slot.into())
    }
}

const _: () = {
    assert!(config::MAX_SERVERS == 4);
    assert!(mem::size_of::<Server>() == mem::size_of::<u8>());
    assert!(mem::size_of::<Option<Server>>() == mem::size_of::<u8>());
};
