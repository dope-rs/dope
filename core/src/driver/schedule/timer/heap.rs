use std::{mem, num, time};

use o3::{
    cell::region,
    collections::{self, heap},
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    pub(super) deadline: time::Instant,
    pub(super) epoch: u32,
}

pub(super) struct Heap<'d> {
    heap: region::Value<'d, heap::Min<Key>>,
}

const _: () = assert!(
    mem::size_of::<region::Value<'static, heap::Min<Key>>>() == mem::size_of::<heap::Min<Key>>()
);

impl<'d> Heap<'d> {
    pub(super) fn empty(token: &region::Token<'d>) -> Self {
        use o3::collections::heap::Min;
        Self::with_heap(token, Min::new())
    }

    pub(super) fn try_new(
        token: &region::Token<'d>,
        capacity: num::NonZeroUsize,
    ) -> Result<Self, collections::AllocationError> {
        use o3::collections::heap::Min;
        Ok(Self::with_heap(
            token,
            Min::try_with_capacity(capacity.get())?,
        ))
    }

    fn with_heap(token: &region::Token<'d>, heap: heap::Min<Key>) -> Self {
        let _ = token;
        Self {
            heap: region::Value::new(heap),
        }
    }

    pub(super) fn remove(&self, token: &mut region::Token<'d>, slot: usize) {
        self.heap.borrow_mut(token).remove(slot);
    }

    pub(super) fn insert(&self, token: &mut region::Token<'d>, slot: usize, key: Key) {
        let Some(entry) = self.heap.borrow_mut(token).vacant_entry(slot) else {
            use std::process;
            process::abort();
        };
        entry.insert(key);
    }

    pub(super) fn peek(&self, token: &region::Token<'d>) -> Option<Key> {
        self.heap.borrow(token).peek().map(|(_, key)| *key)
    }

    pub(super) fn pop(&self, token: &mut region::Token<'d>) -> Option<(usize, Key)> {
        self.heap.borrow_mut(token).pop()
    }
}
