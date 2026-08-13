use crate::driver::{
    self, route,
    schedule::ready::{self, credit},
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(in crate::driver) struct Credits<'d>(driver::Reference<'d>);

impl<'d> Credits<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    fn credit(self) -> credit::Credit<'d> {
        credit::Credit::new(&self.0.shared.scheduling.arena)
    }

    pub(in crate::driver) fn arm_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> bool {
        self.credit().arm(key, target.into_credit_token())
    }

    pub(in crate::driver) fn wake_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
        wake: driver::RecvCreditWake,
    ) {
        self.credit().wake(key, target.into_credit_token(), wake);
    }

    pub(in crate::driver) fn retain_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> bool {
        self.credit().retain(key, target.into_credit_token())
    }

    pub(in crate::driver) fn release_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
        wake: driver::RecvCreditWake,
    ) {
        self.credit().release(key, target.into_credit_token(), wake);
    }

    pub(in crate::driver) fn cancel_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> bool {
        self.credit().cancel(key, target.into_credit_token())
    }

    pub(in crate::driver) fn has_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> bool {
        self.credit().held(key, target.into_credit_token())
    }

    pub(in crate::driver) fn take_recv_credit<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> Option<driver::RecvCreditWake> {
        self.credit().take(key, target.into_credit_token())
    }

    pub(in crate::driver) fn arm_fixed_buffer<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> bool {
        self.credit().arm_buffer(key, target.into_credit_token())
    }

    pub(in crate::driver) fn cancel_fixed_buffer<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> bool {
        self.credit().cancel_buffer(key, target.into_credit_token())
    }

    pub(in crate::driver) fn take_fixed_buffer<T: route::Credit<'d>>(
        self,
        key: ready::FixedKey<'d>,
        target: T,
    ) -> Option<driver::RecvBufferCredit<'d>> {
        self.credit()
            .take_buffer(key, target.into_credit_token())
            .then_some(driver::RecvBufferCredit::new(self.0))
    }

    pub(in crate::driver) fn release_recv_buffers(self, count: usize) {
        self.credit().release_buffers(count);
    }

    pub(in crate::driver) fn release_recv_buffer(self) {
        self.credit().release_buffer();
    }
}

const _: () = assert!(
    std::mem::size_of::<Credits<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
);
