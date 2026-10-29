use o3::{
    depot::arena::slab::{entries, external},
    mem,
    mem::lease,
};

use crate::driver::{route, route::table};

type Insertion<'a, T, Tag> = external::Staged<
    mem::Lease<
        entries::Busy<
            'a,
            external::Stamped<T, route::Epoch>,
            { u32::MAX },
            entries::Exclusive<true>,
            { table::PARTITIONS },
        >,
        lease::Open,
    >,
    Tag,
    true,
    external::Logical,
>;

#[repr(transparent)]
pub struct Entry<'a, T, Tag: route::Tag> {
    inner: external::Vacant<
        entries::Pending<
            'a,
            external::Stamped<T, route::Epoch>,
            { u32::MAX },
            entries::Exclusive<true>,
            { table::PARTITIONS },
        >,
        Tag,
        true,
        route::Epoch,
    >,
}

impl<'a, T, Tag: route::Tag> Entry<'a, T, Tag> {
    pub(in crate::driver::route::table) fn new(
        inner: external::Vacant<
            entries::Pending<
                'a,
                external::Stamped<T, route::Epoch>,
                { u32::MAX },
                entries::Exclusive<true>,
                { table::PARTITIONS },
            >,
            Tag,
            true,
            route::Epoch,
        >,
    ) -> Self {
        Self { inner }
    }

    pub fn insert(self, value: T) {
        self.inner.commit(value);
    }

    pub fn stage(self, value: T) -> Staged<'a, T, Tag> {
        Staged::new(self.inner.stage(value))
    }

    pub fn key(&self) -> table::Key<Tag> {
        table::Key::new(self.inner.key())
    }
}

#[repr(transparent)]
pub struct Staged<'a, T, Tag: route::Tag> {
    inner: Insertion<'a, T, Tag>,
}

impl<'a, T, Tag: route::Tag> Staged<'a, T, Tag> {
    pub(in crate::driver::route::table) fn new(inner: Insertion<'a, T, Tag>) -> Self {
        Self { inner }
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn publish(self) {
        self.inner.publish();
    }

    pub fn reject(self) -> T {
        self.inner.reject()
    }
}
