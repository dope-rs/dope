use o3::collections::{self, batch, batch::set};

use crate::{
    config,
    storage::state::{self, indices},
    wire,
};

pub(in crate::storage) struct PendingIndex {
    datagram: set::Set<indices::FamilyKey>,
    streams: [set::Set<indices::FamilyKey>; config::MAX_SERVERS],
}

#[must_use = "a claimed pending key must be committed or restored"]
pub(in crate::storage) struct Claim<'a> {
    set: &'a set::Set<indices::FamilyKey>,
    key: indices::FamilyKey,
    committed: bool,
}

#[derive(Clone, Copy)]
pub(in crate::storage) enum PendingQueue {
    Datagram,
    Stream(indices::ServerIndex),
}

pub(in crate::storage) struct Transactions {
    free: set::Set<wire::TransactionId>,
    owners: Box<[FamilyOwner]>,
    rng: u64,
}

#[must_use = "an allocated DNS transaction must remain owned until release"]
#[repr(transparent)]
pub(in crate::storage) struct TransactionLease(wire::TransactionId);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct FamilyOwner(u16);

impl PendingQueue {
    pub(super) fn matches(self, request: &state::Request) -> bool {
        match self {
            Self::Datagram => request.transport() == state::Transport::Datagram,
            Self::Stream(server) => {
                request.transport() == state::Transport::Stream && request.server() == server
            }
        }
    }
}

impl PendingIndex {
    pub(super) fn try_new(
        bound: indices::IndexBound<indices::FamilyKey>,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            datagram: set::Set::try_with_capacity(bound.capacity())?,
            streams: [
                set::Set::try_with_capacity(bound.capacity())?,
                set::Set::try_with_capacity(bound.capacity())?,
                set::Set::try_with_capacity(bound.capacity())?,
                set::Set::try_with_capacity(bound.capacity())?,
            ],
        })
    }

    pub(super) fn claim(&mut self, queue: PendingQueue) -> Option<Claim<'_>> {
        let set = match queue {
            PendingQueue::Datagram => &mut self.datagram,
            PendingQueue::Stream(server) => &mut self.streams[server.get()],
        };
        let key = set.front()?.take();
        Some(Claim {
            set,
            key,
            committed: false,
        })
    }

    fn set(&self, queue: PendingQueue) -> &set::Set<indices::FamilyKey> {
        match queue {
            PendingQueue::Datagram => &self.datagram,
            PendingQueue::Stream(server) => &self.streams[server.get()],
        }
    }

    pub(super) fn register(&self, key: indices::FamilyKey, request: &state::Request) {
        let queue = match request.transport() {
            state::Transport::Datagram => PendingQueue::Datagram,
            state::Transport::Stream => PendingQueue::Stream(request.server()),
        };
        unsafe { batch::raw::Set::insert_unchecked(self.set(queue), key) };
    }

    pub(super) fn unregister(&self, key: indices::FamilyKey, request: &state::Request) {
        let set = match request.transport() {
            state::Transport::Datagram => &self.datagram,
            state::Transport::Stream => &self.streams[request.server().get()],
        };
        unsafe { batch::raw::Set::remove_unchecked(set, key) };
    }

    pub(super) fn needs_datagram(&self) -> bool {
        !self.datagram.is_empty()
    }

    pub(super) fn needs_stream(&self) -> bool {
        self.streams.iter().any(|pending| !pending.is_empty())
    }
}

impl Claim<'_> {
    pub(super) const fn key(&self) -> indices::FamilyKey {
        self.key
    }

    pub(super) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // SAFETY: claim removed this exact key from this exact set, and
            // the exclusive Ledger borrow prevents a competing insertion.
            unsafe { batch::raw::Set::restore_unchecked(self.set, self.key) };
        }
    }
}

impl FamilyOwner {
    const VACANT: Self = Self(u16::MAX);

    fn get(self) -> Option<indices::FamilyKey> {
        (self != Self::VACANT)
            .then(|| <indices::FamilyKey as indices::DenseIndex>::from_dense(self.0))
    }

    fn occupy(&mut self, owner: indices::FamilyKey) {
        self.0 = owner.dense();
    }

    fn vacate(&mut self) {
        *self = Self::VACANT;
    }
}

impl TransactionLease {
    pub(in crate::storage) const fn id(&self) -> wire::TransactionId {
        self.0
    }
}

impl Transactions {
    pub(super) fn try_new(
        bound: indices::IndexBound<wire::TransactionId>,
    ) -> Result<Self, collections::AllocationError> {
        let capacity = bound.capacity();
        Ok(Self {
            free: bound.try_filled_set()?,
            owners: collections::BoxSliceExt::try_box_with(capacity, |_| FamilyOwner::VACANT)?,
            rng: 1,
        })
    }

    pub(super) fn seed(&mut self, seed: u64) {
        self.rng = seed.max(1);
    }

    pub(super) fn allocate(&mut self, owner: indices::FamilyKey) -> Option<TransactionLease> {
        let mut value = self.rng.max(1);
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        let value = value.max(1);
        let random = value.wrapping_mul(0x2545_f491_4f6c_dd1d);
        let start = wire::TransactionId::from_wire(random as u16);
        let transaction = self.free.pop_from(start)?;
        self.rng = value;
        self.owners[transaction.index()].occupy(owner);
        Some(TransactionLease(transaction))
    }

    pub(super) fn release(&mut self, transaction: TransactionLease) {
        let transaction = transaction.0;
        self.owners[transaction.index()].vacate();
        unsafe { batch::raw::Set::restore_unchecked(&self.free, transaction) };
    }

    pub(super) fn owner(&self, transaction: wire::TransactionId) -> Option<indices::FamilyKey> {
        self.owners[transaction.index()].get()
    }
}

const _: () =
    assert!(std::mem::size_of::<TransactionLease>() == std::mem::size_of::<wire::TransactionId>());
