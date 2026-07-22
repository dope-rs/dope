pub(crate) mod raw;

use std::pin::Pin;
use std::rc::Rc;

use o3::buffer::BlockPool;

use self::raw::state::WireState;

#[derive(Clone)]
pub(super) struct WireArena {
    pool: Pin<Rc<BlockPool>>,
}

impl WireArena {
    pub(super) fn with_capacity(bytes: u32) -> Self {
        Self {
            pool: Rc::pin(BlockPool::new(bytes / BlockPool::CAPACITY as u32)),
        }
    }

    pub(super) fn state(&self) -> WireState {
        WireState::new(self.pool.clone())
    }
}
