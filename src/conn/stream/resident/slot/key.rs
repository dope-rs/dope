use o3::depot;
use o3::depot::index::Stored;
use std::num;

use crate::conn::stream::resident::slot;

// SAFETY: the packed handle preserves its slot generation, reserves zero as
// vacancy, and derives its direct index from the low word.
unsafe impl<Side> depot::raw::Key for slot::Handle<Side> {
    type Raw = u64;

    const VACANT: Self::Raw = 0;

    fn index(self) -> usize {
        self.index() as usize
    }

    fn into_raw(self) -> Self::Raw {
        self.raw().get()
    }

    fn from_raw(raw: Stored<Self>) -> Self {
        // SAFETY: Stored proves this identity was emitted by a live slot Handle.
        let raw = raw.into_raw();
        let raw = unsafe { num::NonZero::<u64>::new_unchecked(raw) };
        slot::Handle::from_raw(raw)
    }
}
