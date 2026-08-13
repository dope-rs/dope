use std::{cell, mem};

use o3::buffer::resident;

pub(super) struct State<B> {
    pub(super) retained: cell::Cell<u32>,
    pub(super) index: u16,
    pub(super) next: cell::Cell<u16>,
    pub(super) buffer: cell::UnsafeCell<mem::MaybeUninit<B>>,
    pub(super) charge: cell::UnsafeCell<Option<resident::Charge>>,
}

pub(super) struct Counters {
    pub(super) free: cell::Cell<u16>,
    pub(super) retained: cell::Cell<u16>,
}

impl Counters {
    pub(super) fn new() -> Self {
        Self {
            free: cell::Cell::new(0),
            retained: cell::Cell::new(0),
        }
    }
}

impl<B> State<B> {
    pub(super) fn new(index: u16, next: u16) -> Self {
        Self {
            retained: cell::Cell::new(0),
            index,
            next: cell::Cell::new(next),
            buffer: cell::UnsafeCell::new(mem::MaybeUninit::uninit()),
            charge: cell::UnsafeCell::new(None),
        }
    }
}
