use dope_sys::{
    io::reactor::read,
    table::{registry, registry::Entry as _},
};
use o3::depot::map::{self, flat};

use crate::reactor::input::{Slot, entry::Id};

type Vacant<'a, V> = map::Vacancy<&'a mut flat::Table<V>, u64>;

pub struct Vacancy<'a, C: read::Context> {
    pub(super) value: Slot<C>,
    pub(super) id: Id,
    pub(super) slot: Vacant<'a, Slot<C>>,
    pub(super) fd: (usize, Vacant<'a, (usize, Id)>),
}

impl<C: read::Context> registry::Vacancy for Vacancy<'_, C> {
    type Echo = C::Key;

    fn insert(self) -> C::Key {
        let echo = self.value.echo();
        let (raw, fd) = self.fd;
        self.slot.insert(self.value);
        fd.insert((raw, self.id));
        echo
    }
}
