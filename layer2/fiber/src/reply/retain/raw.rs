use std::{cell, marker, mem::size_of, ptr};

use o3::{
    base::marker::ThreadBound,
    mem,
    mem::{Rejected, credit::fair},
};

use crate::reply::retain::grant::{self, entries, payload};

pub(in crate::reply) struct Pools(Pool<1, entries::Tag>, Pool<3, payload::Tag>);

/// A lane and its immutable backlink, initialized together in one tail allocation.
pub(in crate::reply) struct State {
    capacity: mem::raw::State<1, entries::Tag>,
    payload: mem::raw::State<3, payload::Tag>,
    pools: ptr::NonNull<Pools>,
}

struct Pool<const N: usize, Tag> {
    shared: cell::Cell<[usize; N]>,
    thread: marker::PhantomData<ThreadBound>,
    domain: marker::PhantomData<fn(Tag) -> Tag>,
}

pub(in crate::reply) struct Lane<'credits, const N: usize, Tag> {
    state: &'credits State,
    domain: marker::PhantomData<fn(Tag) -> Tag>,
}

type Grant<'credits, const N: usize, Tag, A = [usize; N]> =
    mem::raw::Grant<Lane<'credits, N, Tag>, N, A>;

impl Pools {
    pub(in crate::reply) fn shared(&self) -> [usize; 1] {
        self.0.shared()
    }

    pub(in crate::reply) fn new(capacity: grant::Plan<1>, payload: grant::Plan<3>) -> Self {
        Self(Pool::new(capacity), Pool::new(payload))
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
            capacity: mem::raw::State::new(capacity.reserve),
            payload: mem::raw::State::new(payload.reserve),
            pools: ptr::NonNull::from(pools),
        }
    }

    pub(in crate::reply) fn capacity(&self) -> Lane<'_, 1, entries::Tag> {
        Lane {
            state: self,
            domain: marker::PhantomData,
        }
    }

    pub(in crate::reply) fn payload(&self) -> Lane<'_, 3, payload::Tag> {
        Lane {
            state: self,
            domain: marker::PhantomData,
        }
    }

    fn pools(&self) -> &Pools {
        // SAFETY: the immutable header stays alive with this borrowed state.
        unsafe { self.pools.as_ref() }
    }
}

impl<const N: usize, Tag> Pool<N, Tag> {
    pub(in crate::reply) fn new(plan: grant::Plan<N>) -> Self {
        Self {
            shared: cell::Cell::new(plan.shared),
            thread: marker::PhantomData,
            domain: marker::PhantomData,
        }
    }

    pub(in crate::reply) fn shared(&self) -> [usize; N] {
        self.shared.get()
    }
}

impl<const N: usize, Tag> Clone for Lane<'_, N, Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<const N: usize, Tag> Copy for Lane<'_, N, Tag> {}

// SAFETY: State's immutable backlink was written from the fixed tail header;
// every Lane borrows a state in that same allocation, keeping both accounts live.
unsafe impl mem::raw::Source<1> for Lane<'_, 1, entries::Tag> {
    type Tag = entries::Tag;
    type Word = usize;

    unsafe fn accounts(&self) -> mem::raw::Accounts<'_, 1, entries::Tag> {
        (&self.state.pools().0.shared, &self.state.capacity)
    }
}

// SAFETY: the same immutable allocation binding above projects its payload axis;
// the validated layout bounds each reserve plus surplus by usize.
unsafe impl mem::raw::Source<3> for Lane<'_, 3, payload::Tag> {
    type Tag = payload::Tag;
    type Word = usize;

    unsafe fn accounts(&self) -> mem::raw::Accounts<'_, 3, payload::Tag> {
        (&self.state.pools().1.shared, &self.state.payload)
    }
}

impl<'credits, const N: usize, Tag> Lane<'credits, N, Tag>
where
    Self: mem::raw::Source<N, Tag = Tag, Word = usize>,
{
    pub(in crate::reply) fn try_acquire<A: mem::raw::Amount<N, usize>>(
        self,
        amount: A,
    ) -> Result<Grant<'credits, N, Tag, A>, Rejected<A, fair::Cause<usize, N>>> {
        Ok(mem::raw::Grant::from(mem::raw::Charge::try_new(
            self, amount,
        )?))
    }
}

const _: () = {
    assert!(size_of::<Pool<1, ()>>() == size_of::<usize>());
    assert!(size_of::<Pool<3, ()>>() == 3 * size_of::<usize>());
    assert!(size_of::<State>() == 9 * size_of::<usize>());
};
