use std::{marker, mem, num};

use dope_core::driver::route::{Identity, KeyTag};

/// Driver- and route-bound identity of one queued payload.
///
/// ```compile_fail
/// use dope_core::driver::route::KeyTag;
/// use dope_manifold::network::datagram::send::owners::Queued;
///
/// fn erase(owner: Queued<'_, KeyTag<1>>) -> Queued<'static, KeyTag<1>> {
///     owner
/// }
/// ```
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Queued<'d, ID: Identity> {
    serial: num::NonZero<u64>,
    driver: marker::PhantomData<fn(o3::base::cell::region::Identity<'d>) -> ID>,
}

impl<'d, ID: Identity> Queued<'d, ID> {
    pub(in crate::network::datagram) const fn new(serial: num::NonZero<u64>) -> Self {
        Self {
            serial,
            driver: marker::PhantomData,
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Queued<'static, KeyTag<0>>>() == mem::size_of::<u64>());
    assert!(mem::size_of::<Option<Queued<'static, KeyTag<0>>>>() == mem::size_of::<u64>());
};
