use std::mem;

use dope_core::driver::route::{self, Identity, KeyTag, kind::Send};

/// Driver- and route-bound identity of one payload in kernel flight.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Flight<'d, ID: Identity>(
    route::Operation<'d, <ID as route::Materialize<Send>>::With, Send>,
);

impl<'d, ID: Identity> Flight<'d, ID> {
    pub(in crate::network::datagram) const fn new(
        target: route::Operation<'d, <ID as route::Materialize<Send>>::With, Send>,
    ) -> Self {
        Self(target)
    }
}

const _: () = {
    assert!(mem::size_of::<Flight<'static, KeyTag<0>>>() == mem::size_of::<route::Token>());
};
