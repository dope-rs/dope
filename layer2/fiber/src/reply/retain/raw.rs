use std::{cell, marker, mem::size_of, ptr};

use o3::{
    base::marker::ThreadBound,
    mem::{credit::fair, raw::Balance},
};

use crate::reply::retain::grant;

pub(in crate::reply) struct Pools {
    capacity: cell::Cell<[usize; 1]>,
    payload: cell::Cell<[usize; 3]>,
    thread: marker::PhantomData<ThreadBound>,
}

/// A lane and its immutable backlink, initialized together in one tail allocation.
pub(in crate::reply) struct State {
    capacity: Account<1>,
    payload: Account<3>,
    pools: ptr::NonNull<Pools>,
}

struct Account<const N: usize> {
    reserve: [usize; N],
    held: cell::Cell<[usize; N]>,
}

impl Pools {
    pub(in crate::reply) fn shared(&self) -> [usize; 1] {
        self.capacity.get()
    }

    pub(in crate::reply) fn new(capacity: grant::Plan<1>, payload: grant::Plan<3>) -> Self {
        Self {
            capacity: cell::Cell::new(capacity.shared),
            payload: cell::Cell::new(payload.shared),
            thread: marker::PhantomData,
        }
    }
}

impl State {
    /// # Safety
    /// The state and pools must be initialized in the same immovable allocation;
    /// all state borrows must keep that complete allocation alive.
    pub(in crate::reply) unsafe fn new(
        capacity: grant::Plan<1>,
        payload: grant::Plan<3>,
        pools: &Pools,
    ) -> Self {
        Self {
            capacity: Account::new(capacity.reserve),
            payload: Account::new(payload.reserve),
            pools: ptr::NonNull::from(pools),
        }
    }

    pub(in crate::reply) fn has_reserve(&self) -> bool {
        self.capacity.held.get()[0] < self.capacity.reserve[0]
    }

    pub(in crate::reply) fn is_reserved(&self) -> bool {
        self.capacity.held.get() == self.capacity.reserve
    }

    pub(in crate::reply) fn can_admit(&self, amount: [usize; 1]) -> bool {
        self.capacity.can_acquire(&self.pools().capacity, amount)
    }

    pub(in crate::reply) fn try_admit(
        &self,
        amount: [usize; 1],
    ) -> Result<(), fair::Cause<usize, 1>> {
        self.capacity.try_acquire(&self.pools().capacity, amount)
    }

    /// # Safety
    /// The consuming Payload owns this state's original entry admission.
    pub(in crate::reply) unsafe fn release_slot(&self) {
        unsafe { self.capacity.release(&self.pools().capacity, [1]) };
    }

    pub(in crate::reply) fn can_acquire(&self, amount: [usize; 3]) -> bool {
        self.payload.can_acquire(&self.pools().payload, amount)
    }

    pub(in crate::reply) fn try_acquire(
        &self,
        amount: [usize; 3],
    ) -> Result<(), fair::Cause<usize, 3>> {
        self.payload.try_acquire(&self.pools().payload, amount)
    }

    /// # Safety
    /// The private owner consumes this state's exact outstanding amount once.
    pub(in crate::reply) unsafe fn release(&self, amount: [usize; 3]) {
        unsafe { self.payload.release(&self.pools().payload, amount) };
    }

    fn pools(&self) -> &Pools {
        // SAFETY: the immutable header stays alive with this borrowed state.
        unsafe { self.pools.as_ref() }
    }
}

impl<const N: usize> Account<N> {
    const fn new(reserve: [usize; N]) -> Self {
        Self {
            reserve,
            held: cell::Cell::new([0; N]),
        }
    }

    fn can_acquire(&self, shared: &cell::Cell<[usize; N]>, amount: [usize; N]) -> bool {
        Balance::new(self.held.get(), shared.get())
            .try_acquire(self.reserve, amount)
            .is_ok()
    }

    fn try_acquire(
        &self,
        shared: &cell::Cell<[usize; N]>,
        amount: [usize; N],
    ) -> Result<(), fair::Cause<usize, N>> {
        let next = Balance::new(self.held.get(), shared.get()).try_acquire(self.reserve, amount)?;
        self.commit(shared, next);
        Ok(())
    }

    unsafe fn release(&self, shared: &cell::Cell<[usize; N]>, amount: [usize; N]) {
        // SAFETY: the owning State consumes a disjoint outstanding acquisition;
        // conserved shared capacity bounds the refund in every return order.
        let next = unsafe {
            Balance::new(self.held.get(), shared.get()).release_raw(self.reserve, amount)
        };
        self.commit(shared, next);
    }

    fn commit(&self, shared: &cell::Cell<[usize; N]>, next: Balance<N, usize>) {
        let (held, returned) = next.into_parts();
        self.held.set(held);
        shared.set(returned);
    }
}

const _: () = {
    assert!(size_of::<Pools>() == 4 * size_of::<usize>());
    assert!(size_of::<State>() == 9 * size_of::<usize>());
};
