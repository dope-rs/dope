use dope_sys::io::reactor::complete::{
    self,
    packet::{record, record::Selection},
};

use crate::reactor::cq::{Flags, Outcome};

pub struct Decoded<K, E, B> {
    parts: complete::Parts<K, Outcome, Flags>,
    buffer: Selection<B, E>,
}

impl<K, E, B> complete::Map for Decoded<K, E, B> {
    type Entry<T> = Decoded<K, E, T>;

    fn map<T, U>(entry: Self::Entry<T>, map: impl FnOnce(T) -> U) -> Self::Entry<U> {
        Decoded {
            parts: entry.parts,
            buffer: match entry.buffer {
                Selection::None => Selection::None,
                Selection::Selected(buffer) => Selection::Selected(map(buffer)),
                Selection::Invalid(fault) => Selection::Invalid(fault),
            },
        }
    }
}

impl<K: Copy, E, B> record::Map for Decoded<K, E, B> {
    type Parts = complete::Parts<K, Outcome, Flags>;
    type Fault = E;

    fn new<T>(parts: Self::Parts, buffer: Selection<T, E>) -> Self::Entry<T> {
        Decoded { parts, buffer }
    }

    fn into_parts<T>(entry: Self::Entry<T>) -> (Self::Parts, Selection<T, E>) {
        (entry.parts, entry.buffer)
    }
}
