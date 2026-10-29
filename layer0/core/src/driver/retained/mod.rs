//! Retained-owner driver access.

use std::ops;

use crate::driver;

pub mod accept;
pub mod arm;
pub mod flight;
pub mod raw;

/// Driver access carrying retained-owner authority with the same representation
/// as [`driver::Context`]. Submitting borrowed storage still requires the exact
/// resource-retention proof documented at each raw submission boundary.
#[repr(transparent)]
pub struct Context<'a, 'owner, 'd: 'owner> {
    pub(in crate::driver) driver: driver::Context<'a, 'd>,
    owner: raw::Proof<'owner, 'd>,
}

impl<'a, 'owner, 'd: 'owner> Context<'a, 'owner, 'd> {
    /// Joins driver access with a validated owner proof.
    pub fn new(driver: driver::Context<'a, 'd>, owner: raw::Owner<'owner, 'd>) -> Self {
        Self {
            driver,
            owner: owner.into_proof(),
        }
    }

    pub(crate) const fn token(&self) -> raw::Token<'owner, 'd> {
        raw::Token::new(self.owner)
    }

    pub fn reborrow(&mut self) -> Context<'_, 'owner, 'd> {
        Context {
            driver: self.driver.reborrow(),
            owner: self.owner,
        }
    }

    pub fn driver(&mut self) -> &mut driver::Context<'a, 'd> {
        &mut self.driver
    }

    /// Drops the owner proof while preserving the exact driver turn.
    pub fn into_driver(self) -> driver::Context<'a, 'd> {
        self.driver
    }
}

impl<'a, 'owner, 'd: 'owner> ops::Deref for Context<'a, 'owner, 'd> {
    type Target = driver::Context<'a, 'd>;

    fn deref(&self) -> &Self::Target {
        &self.driver
    }
}

impl<'a, 'owner, 'd: 'owner> ops::DerefMut for Context<'a, 'owner, 'd> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.driver
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Context<'static, 'static, 'static>>()
            == std::mem::size_of::<driver::Context<'static, 'static>>()
    );
    assert!(
        std::mem::align_of::<Context<'static, 'static, 'static>>()
            == std::mem::align_of::<driver::Context<'static, 'static>>()
    );
};
