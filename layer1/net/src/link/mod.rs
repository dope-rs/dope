use std::mem;

use dope_core::{
    driver::{retained, retained::flight, route, route::Identity},
    io::{
        fd::handles,
        socket,
        socket::{option, pending},
        transfer,
    },
};
use setup::Setup;

mod duplex;
pub mod egress;
pub mod event;
pub mod oneshot;
pub mod pool;
mod setup;
pub mod slot;

/// Bytes proven to fit in the exact plain view handed to the wire.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Consumed(transfer::Len);

impl Consumed {
    pub const ZERO: Self = Self(transfer::Len::ZERO);

    pub const fn into_len(self) -> transfer::Len {
        self.0
    }

    pub(crate) const fn proven(amount: transfer::Len) -> Self {
        Self(amount)
    }
}

const _: () = assert!(mem::size_of::<Consumed>() == mem::size_of::<transfer::Len>());

pub(crate) struct Connect<'addr, 'd, ID: Identity, A: socket::Prepared> {
    fd: handles::Descriptor<'d>,
    addr: &'addr A,
    target: route::Target<'d, ID>,
}

impl<'addr, 'd, ID: Identity, A: socket::Prepared> Connect<'addr, 'd, ID, A> {
    pub(crate) fn new(
        fd: handles::Descriptor<'d>,
        addr: &'addr A,
        target: route::Target<'d, ID>,
    ) -> Self {
        Self { fd, addr, target }
    }

    pub(crate) fn submit<'owner>(
        self,
        slots: &flight::Slots<'d, ID>,
        driver: retained::Context<'_, 'owner, 'd>,
        opts: option::Set,
    ) -> Result<pending::Connection<'d>, pending::Rejection<'d>>
    where
        'd: 'owner,
        'owner: 'addr,
    {
        let Self { fd, addr, target } = self;
        // SAFETY: the address is already installed in its slab-indexed stable
        // owner. That owner and this descriptor remain live through terminal
        // completion or quiescence.
        let op = retained::raw::Connect::new(fd, addr, target);
        unsafe { op.try_connect(driver, slots, &opts) }
    }
}
