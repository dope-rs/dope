use dope_sys::io::reactor::retry;
use o3::depot::map::{self, flat};

pub struct Vacancy<'a, E: retry::Entry> {
    pub(super) pending: E,
    pub(super) task: Reserved<'a, E::Record>,
    pub(super) fd: Reserved<'a, E::Target>,
}

/// The retained index key and its exact exclusively borrowed insertion slot.
type Reserved<'a, V> = (usize, map::Vacancy<&'a mut flat::Table<(usize, V)>, u64>);

impl<E: retry::Entry> retry::Vacancy for Vacancy<'_, E> {
    fn commit(self) {
        let (target, record) = self.pending.into_parts();
        let (key, task) = self.task;
        let (raw, fd) = self.fd;
        task.insert((key, record));
        fd.insert((raw, target));
    }
}
