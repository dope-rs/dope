use dope_sys::memory::Context;
use o3::buffer::storage::provided;

use crate::{driver, driver::settings};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Access<'d>(driver::Reference<'d>);

impl<'d> Access<'d> {
    pub(in crate::driver) const fn new(driver: driver::Reference<'d>) -> Self {
        Self(driver)
    }

    pub(crate) fn owners(self) -> provided::Root<'d, driver::Buffer, driver::Shared, ()> {
        self.0.shared
    }

    pub(crate) fn exhausted(self) -> &'d crate::Permits {
        &self.0.shared.context().receive
    }

    pub(crate) fn layout(self) -> settings::Receive {
        self.owners().context().buffer().layout().0
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Access<'static>>() == std::mem::size_of::<driver::Reference<'static>>()
    );
    assert!(
        std::mem::align_of::<Access<'static>>()
            == std::mem::align_of::<driver::Reference<'static>>()
    );
};
