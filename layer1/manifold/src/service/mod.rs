use o3::{
    alloc,
    depot::{array, heap},
};

pub mod bindings;
pub mod observe;
mod opening;
mod pool;

pub(crate) use opening::Opening;

pub mod discover;
pub mod health;
pub mod sync;

pub(crate) use pool::Pool;

/// Exact failure to allocate service scheduling state.

#[derive(Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Heap(heap::Error),
}

/// Absolute endpoint ceiling for one atomically reconciled service snapshot.
pub const MAX_ENDPOINTS: usize = 16;

struct Limit<const N: usize>;

impl<const N: usize> Limit<N> {
    const BOUNDED: () = assert!(
        N <= MAX_ENDPOINTS,
        "service endpoint capacity exceeds MAX_ENDPOINTS"
    );
}

/// Stable identity plus the concrete transport address selected by discovery.
pub struct Endpoint<I, A> {
    id: I,
    addr: A,
}

impl<I, A> Endpoint<I, A> {
    pub const fn new(id: I, addr: A) -> Self {
        Self { id, addr }
    }

    pub const fn id(&self) -> &I {
        &self.id
    }

    pub(crate) fn into_parts(self) -> (I, A) {
        (self.id, self.addr)
    }
}

/// Monotonic generation of an atomically published endpoint set.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Revision(u64);

/// Exact terminal value observed when a service revision cannot advance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exhausted;

const _: () = {
    assert!(std::mem::size_of::<Revision>() == std::mem::size_of::<u64>());
    assert!(std::mem::align_of::<Revision>() == std::mem::align_of::<u64>());
    assert!(std::mem::size_of::<Exhausted>() == 0);
};

impl Revision {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn try_next(self) -> Result<Self, Exhausted> {
        match self.0.checked_add(1) {
            Some(value) => Ok(Self(value)),
            None => Err(Exhausted),
        }
    }
}

/// Immutable desired endpoint state published as one transaction.
pub struct Snapshot<E, const N: usize = 16> {
    revision: Revision,
    endpoints: array::Vec<E, N>,
}

impl<E, const N: usize> Snapshot<E, N> {
    /// Builds an empty inline snapshot at `rev`.
    pub fn empty(rev: Revision) -> Self {
        let () = Limit::<N>::BOUNDED;
        Self {
            revision: rev,
            endpoints: array::Vec::new(),
        }
    }

    /// Builds an inline snapshot containing exactly one endpoint.
    pub fn from_one(rev: Revision, endpoint: E) -> Self {
        let () = Limit::<N>::BOUNDED;
        Self {
            revision: rev,
            endpoints: array::Vec::from_one(endpoint),
        }
    }

    /// Adopts an inline vector whose capacity proof is already established.
    pub fn from_vec(rev: Revision, values: array::Vec<E, N>) -> Self {
        let () = Limit::<N>::BOUNDED;
        Self {
            revision: rev,
            endpoints: values,
        }
    }

    pub const fn revision(&self) -> Revision {
        self.revision
    }

    pub fn endpoints(&self) -> &[E] {
        self.endpoints.as_slice()
    }
}

impl<E, const N: usize> IntoIterator for Snapshot<E, N> {
    type Item = E;
    type IntoIter = <array::Vec<E, N> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.endpoints.into_iter()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Change {
    pub added: usize,
    pub retained: usize,
    pub retired: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reconcile {
    Stale {
        current: Revision,
        incoming: Revision,
    },
    Duplicate {
        first: usize,
        duplicate: usize,
    },
    Capacity {
        actual: usize,
        limit: usize,
    },
}
