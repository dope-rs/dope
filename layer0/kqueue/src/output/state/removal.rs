use dope_sys::io::reactor::retry;
use o3::depot::map::{Found, flat};

pub struct Removal<'a, E: retry::Entry> {
    pub(super) task: Found<&'a mut flat::Table<(usize, E::Record)>>,
    pub(super) fd: Found<&'a mut flat::Table<(usize, E::Target)>>,
}

impl<E: retry::Entry> retry::Occupied for Removal<'_, E> {
    type Entry = E;

    fn record(&self) -> &E::Record {
        &self.task.get().1
    }

    fn remove(self) -> E {
        let (_, target) = self.fd.remove();
        E::from_parts(target, self.task.remove().1)
    }
}
