mod state;

pub(super) use self::state::WireState;
use super::{WireLease, WirePool};

pub(super) struct WireArena<'pool> {
    pool: &'pool WirePool,
}

impl<'pool> WireArena<'pool> {
    pub(super) fn new(pool: &'pool WirePool) -> Self {
        Self { pool }
    }

    pub(super) fn state<'a>(
        &'a self,
        lease: &'a mut Option<WireLease<'pool>>,
    ) -> WireState<'a, 'pool> {
        WireState::new(self.pool, lease)
    }
}
