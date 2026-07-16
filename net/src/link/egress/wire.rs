use std::pin::Pin;
use std::rc::Rc;

use o3::buffer::{BlockLease, BlockPool};

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

    pub(super) fn acquire(&self) -> Option<WireBuf> {
        let lease = self.pool.as_ref().try_acquire()?;
        Some(unsafe { std::mem::transmute::<BlockLease<'_>, BlockLease<'static>>(lease) })
    }
}

pub(super) type WireBuf = BlockLease<'static>;
