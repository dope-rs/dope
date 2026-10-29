use std::{cell, mem};

use dope_core::driver::{
    route,
    route::{Identity, KeyTag},
};

use crate::link::pool;

pub(in crate::link::pool::pending) struct Node<'d, ID: Identity> {
    pub(in crate::link::pool::pending) epoch: cell::Cell<route::Epoch>,
    pub(in crate::link::pool::pending) next: cell::Cell<Next<'d, ID>>,
    pub(in crate::link::pool::pending) state: cell::Cell<State>,
    pub(in crate::link::pool::pending) wake: cell::Cell<Wake>,
    pub(in crate::link::pool::pending) capacity: cell::Cell<bool>,
}

#[derive(Clone, Copy)]
pub(in crate::link::pool::pending) enum Next<'d, ID: Identity> {
    End,
    Slot(pool::raw::Physical<'d, ID>),
}

#[derive(Clone, Copy)]
pub(in crate::link::pool::pending) enum State {
    Idle,
    Queued,
    Claimed,
    Retired,
}

#[derive(Clone, Copy)]
pub(in crate::link::pool::pending) enum Wake {
    Quiet,
    Pending,
}

impl<'d, ID: Identity> Default for Node<'d, ID> {
    fn default() -> Self {
        Self {
            epoch: cell::Cell::new(route::Epoch::INITIAL),
            next: cell::Cell::new(Next::End),
            state: cell::Cell::new(State::Idle),
            wake: cell::Cell::new(Wake::Quiet),
            capacity: cell::Cell::new(false),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Next<'static, KeyTag<0>>>() == mem::size_of::<u32>());
    assert!(mem::size_of::<State>() == 1);
    assert!(mem::size_of::<Wake>() == 1);
    assert!(mem::size_of::<Node<'static, KeyTag<0>>>() == 2 * mem::size_of::<u64>());
};
