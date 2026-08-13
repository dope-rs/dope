use core::{marker, mem};

use crate::driver::{
    self,
    route::{self, table},
};

/// Zero-sized constructor branding table identities with one driver.
#[derive(Clone, Copy)]
pub struct Space<'d, Tag: route::Tag> {
    pub(in crate::driver) driver: route::Brand<'d, Tag>,
}

impl<'d, Tag: route::Tag> Space<'d, Tag> {
    #[doc(hidden)]
    pub const fn for_driver(_driver: driver::Reference<'d>) -> Self {
        Self {
            driver: marker::PhantomData,
        }
    }

    pub const fn bind_key(self, key: table::Key<Tag>) -> route::Target<'d, Tag> {
        self.bind_parts(key.parts())
    }

    pub const fn bind(self, slot: route::SlotIndex, epoch: route::Epoch) -> route::Target<'d, Tag> {
        self.bind_parts(table::Parts::from_components(slot, epoch))
    }

    #[doc(hidden)]
    pub const fn bind_parts(self, parts: table::Parts<Tag>) -> route::Target<'d, Tag> {
        let _ = self;
        route::Target::from_parts(parts)
    }

    pub const fn parse(self, token: route::Token) -> Option<route::Target<'d, Tag>> {
        match token.parts::<Tag>() {
            Some(parts) => Some(self.bind_parts(parts)),
            None => None,
        }
    }
}

const _: () = assert!(mem::size_of::<Space<'static, route::KeyTag<1>>>() == 0);
