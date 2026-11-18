use std::{array, mem};

use o3::mem::raw::Amount;

use crate::reply::usage;

pub(in crate::reply) struct Sum([usize; 3]);

impl Sum {
    pub(in crate::reply) const fn new() -> Self {
        Self([0; 3])
    }

    /// # Safety
    /// These are the checked totals after the original source admitted the member.
    pub(in crate::reply) unsafe fn commit(&mut self, items: usize, usage: usage::Usage) {
        self.0 = [items, usage.values().get(), usage.bytes().get()];
    }

    pub(in crate::reply) fn len(&self) -> usize {
        self.0[0]
    }

    pub(in crate::reply) fn usage(&self) -> usage::Usage {
        usage::Usage::new(usage::Bytes::new(self.0[2]), usage::Values::new(self.0[1]))
    }

    /// # Safety
    /// The amount belongs to one still-accounted original member of this sum.
    pub(in crate::reply) unsafe fn take<A: Amount<3, usize>>(&mut self, amount: A) -> A {
        let part = amount.get();
        self.0 = array::from_fn(|axis| self.0[axis].wrapping_sub(part[axis]));
        amount
    }
}

const _: () = assert!(mem::size_of::<Sum>() == 3 * mem::size_of::<usize>());
