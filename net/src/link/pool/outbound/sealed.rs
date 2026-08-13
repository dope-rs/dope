use std::{marker, mem};

use dope_core::{driver::route, io::socket};
use o3::collections;

use crate::link::pool::{self, pending};

pub(in crate::link::pool) struct AddressTable<'d, const ID: u8> {
    slots: Box<[mem::MaybeUninit<socket::Addr>]>,
    _route: marker::PhantomData<fn(&'d route::KeyTag<ID>) -> &'d route::KeyTag<ID>>,
}

pub(in crate::link) struct StoredAddress<'d, const ID: u8> {
    _route: marker::PhantomData<fn(&'d route::KeyTag<ID>) -> &'d route::KeyTag<ID>>,
}

impl<'d, const ID: u8> AddressTable<'d, ID> {
    pub(in crate::link::pool) fn try_with_capacity(
        capacity: u32,
    ) -> Result<Self, collections::AllocationError> {
        let slots = collections::BoxSliceExt::try_box_with(capacity as usize, |_| {
            mem::MaybeUninit::uninit()
        })?;
        Ok(Self {
            slots,
            _route: marker::PhantomData,
        })
    }

    pub(super) fn store<U>(
        &mut self,
        vacancy: &pending::Vacancy<'_, 'd, ID, U>,
        addr: socket::Addr,
    ) -> StoredAddress<'d, ID> {
        let index = vacancy.index().raw() as usize;
        unsafe { self.slots.get_unchecked_mut(index) }.write(addr);
        StoredAddress {
            _route: marker::PhantomData,
        }
    }

    pub(super) fn get(&self, key: pool::Key<'d, ID>) -> &socket::Addr {
        unsafe { self.slots.get_unchecked(key.index()).assume_init_ref() }
    }
}

const _: () =
    assert!(mem::size_of::<mem::MaybeUninit<socket::Addr>>() == mem::size_of::<socket::Addr>());
const _: () = assert!(mem::size_of::<AddressTable<'static, 0>>() == 2 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<StoredAddress<'static, 0>>() == 0);
