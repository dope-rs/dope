use o3::depot::arena::slab::{entries, external};

use crate::driver::{route, route::table::Key};

pub struct Current<'a, T, Tag: route::Tag>(
    external::Borrowed<
        entries::Busy<
            'a,
            external::Stamped<T, route::Epoch>,
            { u32::MAX },
            entries::Interior<true>,
            1,
        >,
        Tag,
        true,
    >,
);

impl<'a, T, Tag: route::Tag> Current<'a, T, Tag> {
    pub(in crate::driver::route) fn new(
        entry: external::Borrowed<
            entries::Busy<
                'a,
                external::Stamped<T, route::Epoch>,
                { u32::MAX },
                entries::Interior<true>,
                1,
            >,
            Tag,
            true,
        >,
    ) -> Self {
        Self(entry)
    }

    pub fn key(&self) -> Key<Tag> {
        Key::new(self.0.key())
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    pub fn take(self) -> T {
        self.0.take()
    }

    pub fn discard(self) {
        self.0.discard();
    }
}
