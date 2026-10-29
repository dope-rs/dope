use core::marker::PhantomData;
use std::{mem, ops};

use o3::mem::{Lease, Return, lease::Open};

use crate::{
    driver,
    driver::{
        life, route,
        route::{lease, queues},
    },
};

/// A live lease already owns this driver-local route identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Occupied,
}

const _: () = assert!(std::mem::size_of::<Error>() == 0);

pub(crate) struct Registry {
    live: [u64; route::CAPACITY / u64::BITS as usize],
}

pub(in crate::driver::route) struct Reserved<'d, B: route::App> {
    brand: PhantomData<B>,
    driver: driver::Reference<'d>,
}

impl Registry {
    pub(crate) const fn new() -> Self {
        Self {
            live: [0; route::CAPACITY / u64::BITS as usize],
        }
    }

    pub(in crate::driver) fn release<'d>(
        local: &mut life::Local<'d>,
        retired: queues::Retired<'d>,
    ) {
        let id = retired.into_id();
        local.routes.live[id.raw() as usize / 64] &= !(1u64 << (id.raw() % 64));
    }
}

impl<'d, B: route::App> Reserved<'d, B> {
    pub(super) fn try_new(local: &mut life::Local<'d>) -> Result<Self, lease::Error> {
        let id = <B as route::App>::ID;
        let word = id.raw() as usize / 64;
        let mask = 1u64 << (id.raw() % 64);
        if local.routes.live[word] & mask != 0 {
            return Err(lease::Error::Occupied);
        }
        local.routes.live[word] |= mask;
        Ok(Self {
            driver: local.driver,
            brand: PhantomData,
        })
    }

    pub(in crate::driver::route) fn driver(&self) -> driver::Reference<'d> {
        self.driver
    }

    pub(super) fn release(self, driver: &mut driver::Context<'_, 'd>) {
        driver.storage.release(queues::Retired::reserved(self));
    }
}

impl<B: route::App> Return for Reserved<'_, B> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        queues::Pending::defer(self);
    }
}

/// A route identity reserved by non-submitting construction and not yet owned.
///
///
pub struct Claim<'d, B: route::App> {
    driver: Lease<Reserved<'d, B>, Open>,
}

impl<'d, B: route::App> Claim<'d, B> {
    pub(in crate::driver) fn try_new(local: &mut life::Local<'d>) -> Result<Self, Error> {
        Ok(Self {
            driver: Lease::open(Reserved::try_new(local)?, ()),
        })
    }

    pub fn into_owner(self) -> Owner<'d, B> {
        self.store(())
    }

    pub fn store<T>(self, value: T) -> Owner<'d, B, T> {
        Owner::new(self, value)
    }

    pub fn driver(&self) -> driver::Reference<'d> {
        self.driver.sink().driver()
    }

    /// Releases this reservation immediately through its branded driver.
    pub fn rollback(self, driver: &mut driver::Context<'_, 'd>) {
        let (reserved, ()) = self.driver.into_parts();
        reserved.release(driver);
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Claim<'static, route::KeyTag<0>>>() == word);
    assert!(mem::size_of::<Option<Claim<'static, route::KeyTag<0>>>>() == word);
};

/// Affine ownership of one driver-local route identity and its protected value.
///
///
pub struct Owner<'d, B: route::App, T = ()> {
    value: T,
    claim: Claim<'d, B>,
}

impl<'d, B: route::App> Owner<'d, B> {
    /// Reserves this route while borrowing the matching driver context.
    pub fn try_reserve<'a, 'c>(
        driver: &'a mut driver::Context<'c, 'd>,
    ) -> Result<Vacant<'a, 'c, 'd, B>, Error> {
        let local = &mut *driver.storage.0;
        let claim = Claim::try_new(local)?;
        Ok(Vacant::new(claim, driver))
    }

    pub fn release(self, driver: &mut driver::Context<'_, 'd>) {
        self.claim.rollback(driver);
    }
}

impl<'d, B: route::App, T> Owner<'d, B, T> {
    pub fn driver(&self) -> driver::Reference<'d> {
        self.claim.driver()
    }

    pub(super) fn new(claim: Claim<'d, B>, value: T) -> Self {
        Self { value, claim }
    }
}

impl<B: route::App, T> ops::Deref for Owner<'_, B, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

const _: () = {
    assert!(mem::size_of::<Owner<'static, route::KeyTag<0>>>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Owner<'static, route::KeyTag<0>>>() == mem::align_of::<usize>());
    assert!(
        mem::size_of::<Owner<'static, route::KeyTag<0>, usize>>()
            == mem::size_of::<(Owner<'static, route::KeyTag<0>>, usize)>()
    );
};

/// A rollback-safe reservation that temporarily borrows its driver context.
pub struct Vacant<'a, 'c, 'd, B: route::App> {
    hold: Lease<Rollback<'a, 'c, 'd, B>, Open>,
}

struct Rollback<'a, 'c, 'd, B: route::App>(&'a mut driver::Context<'c, 'd>, PhantomData<B>);
impl<'a, 'c, 'd, B: route::App> Vacant<'a, 'c, 'd, B> {
    pub(in crate::driver) fn new(
        hold: Claim<'d, B>,
        driver: &'a mut driver::Context<'c, 'd>,
    ) -> Self {
        Self {
            hold: Lease::open(Rollback(driver, PhantomData), hold),
        }
    }

    /// Borrows the driver held exclusively by this reservation.
    pub fn driver(&mut self) -> &mut driver::Context<'c, 'd> {
        self.hold.parts_mut().0.0
    }

    /// Commits the reservation into affine route ownership.
    pub fn commit(self) -> Owner<'d, B> {
        self.into_claim().into_owner()
    }

    pub fn into_claim(self) -> Claim<'d, B> {
        self.hold.into_parts().1
    }
}

impl<'d, B: route::App> Return for Rollback<'_, '_, 'd, B> {
    type Item = Claim<'d, B>;

    fn return_item(self, hold: Self::Item) {
        hold.rollback(self.0);
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Vacant<'static, 'static, 'static, route::KeyTag<0>>>() == 2 * word);
    assert!(
        mem::size_of::<Option<Vacant<'static, 'static, 'static, route::KeyTag<0>>>>() == 2 * word
    );
};
