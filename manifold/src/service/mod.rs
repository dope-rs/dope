use std::{convert, error, fmt, time};

use o3::collections::fixed::array;

pub mod attach;
pub mod connector;
mod pool;

pub mod discover;
pub mod health;
pub mod observe;
pub mod reconcile;

pub(crate) use pool::Pool;

/// Absolute endpoint ceiling for one atomically reconciled service snapshot.
pub const MAX_ENDPOINTS: usize = 16;

struct EndpointCount<const N: usize>;

impl<const N: usize> EndpointCount<N> {
    const BOUNDED: () = assert!(
        N <= MAX_ENDPOINTS,
        "service endpoint capacity exceeds MAX_ENDPOINTS"
    );
}

pub struct Fixed<I, A, const N: usize = 16> {
    snapshot: Option<Snapshot<Endpoint<I, A>, N>>,
}

impl<I, A, const N: usize> Fixed<I, A, N> {
    pub const fn new(snapshot: Snapshot<Endpoint<I, A>, N>) -> Self {
        Self {
            snapshot: Some(snapshot),
        }
    }
}

impl<I, A, const N: usize> discover::Discover<I, A, N> for Fixed<I, A, N> {
    type Metadata = ();
    type Error = convert::Infallible;

    fn refresh(&mut self, _now: time::Instant, _reason: discover::Refresh) {}

    fn poll(
        &mut self,
        _now: time::Instant,
    ) -> discover::Action<I, A, Self::Metadata, Self::Error, N> {
        match self.snapshot.take() {
            Some(snapshot) => discover::Action::Published {
                snapshot,
                metadata: (),
                poll_at: None,
            },
            None => discover::Action::Pending { poll_at: None },
        }
    }
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

    pub const fn addr(&self) -> &A {
        &self.addr
    }

    pub(crate) fn into_parts(self) -> (I, A) {
        (self.id, self.addr)
    }
}

/// Monotonic generation of an atomically published endpoint set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Immutable desired endpoint state published as one transaction.
///
/// ```compile_fail,E0080
/// use dope_manifold::service::{Revision, Snapshot};
///
/// let _ = Snapshot::<(), 17>::try_new(Revision::INITIAL, []);
/// ```
pub struct Snapshot<E, const N: usize = 16> {
    revision: Revision,
    endpoints: array::Inline<E, N>,
}

impl<E, const N: usize> Snapshot<E, N> {
    pub(crate) fn empty(revision: Revision) -> Self {
        let () = EndpointCount::<N>::BOUNDED;
        Self {
            revision,
            endpoints: array::Inline::new(),
        }
    }

    /// Builds inline without container allocation, consuming at most `N + 1` items.
    pub fn try_new(
        revision: Revision,
        endpoints: impl IntoIterator<Item = E>,
    ) -> Result<Self, ReconcileError> {
        let () = EndpointCount::<N>::BOUNDED;
        let mut stored = array::Inline::new();
        for endpoint in endpoints {
            stored
                .push(endpoint)
                .map_err(|_| ReconcileError::Capacity { limit: N })?;
        }
        Ok(Self {
            revision,
            endpoints: stored,
        })
    }

    pub const fn revision(&self) -> Revision {
        self.revision
    }

    pub fn endpoints(&self) -> &[E] {
        self.endpoints.as_slice()
    }

    pub(crate) fn into_endpoints(self) -> impl ExactSizeIterator<Item = E> {
        self.endpoints.into_iter()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Change {
    pub added: usize,
    pub retained: usize,
    pub retired: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconcileError {
    Stale {
        current: Revision,
        incoming: Revision,
    },
    Duplicate,
    Capacity {
        limit: usize,
    },
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale { current, incoming } => write!(
                formatter,
                "service snapshot revision {} does not advance current revision {}",
                incoming.get(),
                current.get()
            ),
            Self::Duplicate => {
                formatter.write_str("service snapshot contains a duplicate endpoint")
            }
            Self::Capacity { limit } => {
                write!(formatter, "service snapshot exceeds endpoint limit {limit}")
            }
        }
    }
}

impl error::Error for ReconcileError {}
