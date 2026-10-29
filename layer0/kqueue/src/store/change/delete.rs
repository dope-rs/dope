use dope_sys::table::change;
use o3::depot::index::Ordinal;

use crate::store::change::Changes;

/// The exclusive owner retains a validated per-cell deletion plan separately
/// from live membership. Dropping a plan leaves every live change untouched.
pub struct Delete<'a, S: change::raw::Slots> {
    pub(super) owner: &'a mut Changes<S>,
}

impl<S: change::raw::Slots> change::Delete for Delete<'_, S> {
    fn commit(self) {
        self.owner.remove(usize::MAX);
        while let Some(row) = self.owner.pending.get::<1>().pop_from(Ordinal::new(0)) {
            // SAFETY: this row comes from this owner's immutable bitmap;
            // it has the same extent as this owner's initialized staged array.
            let event = unsafe { *self.owner.staged.get_unchecked(row.index().raw()) };
            self.owner.project(row.into_entry()).set(Some(event));
        }
    }
}

impl<S: change::raw::Slots> Drop for Delete<'_, S> {
    fn drop(&mut self) {
        self.owner.clear_plan();
    }
}
