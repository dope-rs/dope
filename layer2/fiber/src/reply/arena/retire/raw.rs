use o3::depot;

use super::{Retired, State};

impl State {
    pub(in crate::reply) fn push(&self, retired: Retired) {
        // SAFETY: each occupied entry has at most one affine retirement. A popped
        // Draining entry remains occupied, so its reserved ring place is available
        // when requeued even if other entries retire during its step.
        unsafe { depot::raw::RingCell::insert::<false>(&self.queued, retired) };
    }
}
