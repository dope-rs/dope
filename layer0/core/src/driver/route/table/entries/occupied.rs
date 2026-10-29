use o3::depot::arena::slab::{entries, external};

use crate::driver::{route, route::table};

#[repr(transparent)]
pub struct Entry<'a, T, Tag: route::Tag>(
    external::Occupied<
        entries::Busy<
            'a,
            external::Stamped<T, route::Epoch>,
            { u32::MAX },
            entries::Exclusive<true>,
            { table::PARTITIONS },
        >,
        Tag,
        true,
    >,
);

impl<'a, T, Tag: route::Tag> Entry<'a, T, Tag> {
    pub(in crate::driver::route::table) fn new(
        inner: external::Occupied<
            entries::Busy<
                'a,
                external::Stamped<T, route::Epoch>,
                { u32::MAX },
                entries::Exclusive<true>,
                { table::PARTITIONS },
            >,
            Tag,
            true,
        >,
    ) -> Self {
        Self(inner)
    }

    pub fn key(&self) -> table::Key<Tag> {
        table::Key::new(self.0.key())
    }

    pub fn get(&self) -> &T {
        self.0.get()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    pub fn remove(self) -> T {
        self.0.remove()
    }

    /// Destroys the occupied value in place before returning its exact slot.
    pub fn discard(self) {
        self.0.discard();
    }

    pub fn try_remove<R, E>(
        self,
        take: impl FnOnce(T) -> Result<R, (T, E)>,
    ) -> Result<R, (Self, E)> {
        self.0
            .try_remove(take)
            .map_err(|(entry, error)| (Self(entry), error))
    }
}
