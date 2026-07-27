use dope_core::driver::token::{Epoch, SlotIndex, Token};
use o3::collections::FixedQueue;

pub(super) struct Rearm<const ID: u8> {
    pub(super) pending: FixedQueue<SlotIndex>,
    pub(super) epochs: Box<[Epoch]>,
}

impl<const ID: u8> Rearm<ID> {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            pending: FixedQueue::with_capacity(capacity),
            epochs: vec![Epoch::ZERO; capacity].into_boxed_slice(),
        }
    }

    pub(super) fn queue(&mut self, token: Token) {
        debug_assert_eq!(token.route(), ID);
        let index = token.slot().raw() as usize;
        // SAFETY: pool code only queues tokens resolved against the live slab;
        // `Rearm` is allocated with that slab's exact capacity.
        let epoch = unsafe { self.epochs.get_unchecked_mut(index) };
        if *epoch == Epoch::ZERO {
            let Some(entry) = self.pending.vacant_entry() else {
                unreachable!()
            };
            entry.push_back(token.slot());
        }
        *epoch = token.epoch();
    }
}
