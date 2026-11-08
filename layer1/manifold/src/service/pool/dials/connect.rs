use std::mem;

use dope_core::driver::route::{Identity, KeyTag, indices::SlotIndex};

use crate::service::pool::{self, dials};

/// Dropping admission leaves the submitted dial owned by its transport finalizer.
pub(in crate::service::pool) struct Connect<'a, 'q, 'h, 'd, ID: Identity> {
    pub(super) dial: &'a dials::Dial<'q, 'h, 'd, ID>,
    pub(super) target: pool::Target<'h>,
}

impl<'h, ID: Identity> Connect<'_, '_, 'h, '_, ID> {
    pub(in crate::service::pool) const fn target(&self) -> pool::Target<'h> {
        self.target
    }

    pub(in crate::service::pool) fn commit(self, target: pool::Target<'h>) {
        self.dial.state.set(dials::State::Connected(target));
    }
}

const _: () = {
    assert!(
        mem::size_of::<Connect<'static, 'static, 'static, 'static, KeyTag<0>>>()
            == mem::size_of::<(
                &dials::Dial<'static, 'static, 'static, KeyTag<0>>,
                pool::Target<'static>
            )>()
    );
    assert!(
        mem::size_of::<Connect<'static, 'static, 'static, 'static, KeyTag<0>>>()
            <= mem::size_of::<(&mut (), SlotIndex, pool::Target<'static>)>()
    );
};
