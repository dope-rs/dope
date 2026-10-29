use dope_sys::io::reactor::complete::{self, Event};
use o3::depot::queue::ring;

use crate::store::done::Entry;

/// Exclusive reservation of one already allocated completion slot.
pub struct Vacancy<'a, E: Event>(pub(in crate::store::done) ring::Vacant<'a, ring::Fifo<Entry<E>>>);

impl<E: Event> complete::Vacancy for Vacancy<'_, E> {
    type Event = E;

    fn push(self, value: E) {
        self.0.push_back(Entry {
            suppressed: false,
            event: value,
        });
    }
}
