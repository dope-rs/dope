use std::mem;
use std::pin::Pin;
use std::rc::Rc;

use o3::buffer::{BlockLease, BlockPool};

pub(crate) struct WireLease {
    block: BlockLease<'static>,
    _pool: Pin<Rc<BlockPool>>,
}

impl WireLease {
    pub(crate) fn acquire(owner: Pin<Rc<BlockPool>>) -> Option<Self> {
        let block = owner.as_ref().try_acquire()?;
        let block = unsafe { mem::transmute::<BlockLease<'_>, BlockLease<'static>>(block) };
        Some(Self {
            block,
            _pool: owner,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.block.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    pub(crate) fn push(&mut self, byte: u8) -> bool {
        self.block.try_push(byte).is_ok()
    }

    pub(crate) fn extend_from_slice(&mut self, src: &[u8]) -> bool {
        self.block.try_extend_from_slice(src).is_ok()
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        self.block.as_mut_slice()
    }

    pub(crate) fn pointer_at(&self, offset: usize) -> *const u8 {
        debug_assert!(offset <= self.block.len());
        unsafe { self.block.as_ptr().add(offset) }
    }

    pub(crate) fn consume(&mut self, amount: usize) {
        self.block.consume(amount);
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        self.block.truncate(len);
    }
}
