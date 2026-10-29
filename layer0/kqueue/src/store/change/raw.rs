use dope_sys::table::change;
use o3::{depot, depot::index::Ordinal};

use crate::store::change::Changes;

/// # Safety
/// The offset was admitted by the exclusively borrowed file table created
/// with this owner's identical immutable capacity.
pub(crate) unsafe fn discard<S: change::raw::Slots>(changes: &mut Changes<S>, offset: usize) {
    let start = (changes.slots.len() - 1 - offset) * 2;
    for lane in 0..2 {
        // SAFETY: construction allocates exactly two initialized bitmap rows
        // per file slot. The admitted offset bounds both rows in this owner.
        let source =
            unsafe { depot::raw::Proof::new((&changes.pending, Ordinal::new(start + lane))) };
        changes.project(source.into()).set(None);
    }
}
