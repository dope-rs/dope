use std::cell::Cell;
use std::io::{self, Error, ErrorKind};

use crate::driver::control::ContextControl;

use super::token::ROUTE_FRAMEWORK;
use super::{DriverContext, DriverRef};

pub(crate) struct Routes {
    live: [u64; 4],
    poisoned: [u64; 4],
}

impl Routes {
    pub(crate) const fn new() -> Self {
        Self {
            live: [0; 4],
            poisoned: [0; 4],
        }
    }

    pub(crate) fn reserve(&mut self, id: u8) -> bool {
        let word = id as usize / 64;
        let mask = 1u64 << (id % 64);
        if self.live[word] & mask != 0 || self.poisoned[word] & mask != 0 {
            return false;
        }
        self.live[word] |= mask;
        true
    }

    pub(crate) fn release(&mut self, id: u8) {
        self.live[id as usize / 64] &= !(1u64 << (id % 64));
    }

    pub(crate) fn poison(&mut self, id: u8) {
        debug_assert!(!self.is_poisoned(id));
        let word = id as usize / 64;
        let mask = 1u64 << (id % 64);
        self.live[word] &= !mask;
        self.poisoned[word] |= mask;
    }

    pub(crate) fn is_poisoned(&self, id: u8) -> bool {
        self.poisoned[id as usize / 64] & (1u64 << (id % 64)) != 0
    }
}

pub struct Route<'d, const ID: u8> {
    driver: DriverRef<'d>,
    live: Cell<bool>,
}

impl<'d, const ID: u8> Route<'d, ID> {
    pub fn reserve(driver: &mut DriverContext<'_, 'd>) -> io::Result<Self> {
        if ID == ROUTE_FRAMEWORK {
            return Err(Error::new(ErrorKind::InvalidInput, "dope: reserved route"));
        }
        if !driver.reserve_route(ID) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "dope: route already used",
            ));
        }
        Ok(Self {
            driver: driver.driver_ref(),
            live: Cell::new(true),
        })
    }

    pub fn poison(&self, driver: &mut DriverContext<'_, 'd>) {
        if self.live.replace(false) {
            driver.poison_route(ID);
        }
    }

    pub fn driver(&self) -> DriverRef<'d> {
        self.driver
    }

    pub fn release(self, driver: &mut DriverContext<'_, 'd>) {
        if self.live.replace(false) {
            driver.release_route(ID);
        }
    }

    pub fn finish(&self, driver: &mut DriverContext<'_, 'd>, poison: bool) {
        if !self.live.replace(false) {
            return;
        }
        if poison {
            driver.poison_route(ID);
        } else {
            driver.release_route(ID);
        }
    }
}
