use std::mem;

use crate::driver::{self, route, schedule::ready};

#[derive(Clone, Copy)]
pub struct Handle<'d> {
    driver: driver::Reference<'d>,
    key: ready::FixedKey<'d>,
}

impl<'d> Handle<'d> {
    pub(super) const fn new(driver: driver::Reference<'d>, key: ready::FixedKey<'d>) -> Self {
        Self { driver, key }
    }

    pub fn set_target<Tag: route::Tag>(self, target: route::Operation<'d, Tag>) {
        self.driver
            .ready()
            .arena()
            .set_target(self.key.key(), target.into_token());
    }

    pub fn activate(self) {
        self.driver
            .ready()
            .arena()
            .activate_dispatch(self.key.key());
    }

    pub fn key(self) -> ready::Key<'d> {
        self.key.key()
    }

    pub fn target(self) -> ready::Target<'d> {
        ready::Target {
            driver: self.driver,
            key: self.key.key(),
        }
    }

    #[doc(hidden)]
    pub fn identity(self) -> ready::FixedIdentity<'d> {
        ready::FixedIdentity::new(self.key)
    }

    #[doc(hidden)]
    pub fn arm_recv_credit<T: route::Credit<'d>>(self, target: T) -> bool {
        self.driver.credits().arm_recv_credit(self.key, target)
    }

    #[doc(hidden)]
    pub fn wake_recv_credit<T: route::Credit<'d>>(self, target: T, wake: driver::RecvCreditWake) {
        self.driver
            .credits()
            .wake_recv_credit(self.key, target, wake);
    }

    pub fn retain_recv_credit<T: route::Credit<'d>>(self, target: T) -> bool {
        self.driver.credits().retain_recv_credit(self.key, target)
    }

    pub fn release_recv_credit<T: route::Credit<'d>>(
        self,
        target: T,
        wake: driver::RecvCreditWake,
    ) {
        self.driver
            .credits()
            .release_recv_credit(self.key, target, wake);
    }

    #[doc(hidden)]
    pub fn cancel_recv_credit<T: route::Credit<'d>>(self, target: T) -> bool {
        self.driver.credits().cancel_recv_credit(self.key, target)
    }

    #[doc(hidden)]
    pub fn has_recv_credit<T: route::Credit<'d>>(self, target: T) -> bool {
        self.driver.credits().has_recv_credit(self.key, target)
    }

    #[doc(hidden)]
    pub fn take_recv_credit<T: route::Credit<'d>>(
        self,
        target: T,
    ) -> Option<driver::RecvCreditWake> {
        self.driver.credits().take_recv_credit(self.key, target)
    }

    #[doc(hidden)]
    pub fn arm_recv_buffer<T: route::Credit<'d>>(self, target: T) -> bool {
        self.driver.credits().arm_fixed_buffer(self.key, target)
    }

    #[doc(hidden)]
    pub fn cancel_recv_buffer<T: route::Credit<'d>>(self, target: T) -> bool {
        self.driver.credits().cancel_fixed_buffer(self.key, target)
    }

    #[doc(hidden)]
    pub fn take_recv_buffer<T: route::Credit<'d>>(
        self,
        target: T,
    ) -> Option<driver::RecvBufferCredit<'d>> {
        self.driver.credits().take_fixed_buffer(self.key, target)
    }
}

const _: () = {
    assert!(mem::size_of::<ready::Key<'static>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Handle<'static>>() == 2 * mem::size_of::<usize>());
};
