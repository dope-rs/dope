use std::{marker, mem};

use dope_core::driver::route::{self, Identity, KeyTag};

#[derive(Clone, Copy)]
/// Driver-branded proof for receive paths without stored credit.
pub struct Direct<'d, ID: Identity> {
    proof: marker::PhantomData<route::Target<'d, ID>>,
}

impl<'d, ID: Identity> Direct<'d, ID> {
    pub(crate) const NEW: Self = Self {
        proof: marker::PhantomData,
    };
}

const _: () = assert!(mem::size_of::<Direct<'static, KeyTag<0>>>() == 0);
