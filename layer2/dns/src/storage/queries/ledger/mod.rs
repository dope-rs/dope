use std::{mem, net, ops, time};

use o3::{
    alloc::VecExt as _,
    depot::{
        self,
        index::{Initialized, set},
    },
    mem::{Lease, Return, lease::Open},
};

use crate::{
    config,
    query::family,
    storage,
    storage::queries::{index, state, stream, tx},
    wire::tx::Id,
};

mod registry;
mod slot;
pub(in crate::storage::queries) mod starts;
pub(in crate::storage::queries) mod waiting;

pub(in crate::storage::queries) use registry::Registry;
pub(in crate::storage::queries) use slot::Slot;

pub(in crate::storage::queries) type Lane<Q> = depot::raw::Proof<(Q, index::Lane), Initialized<()>>;

pub(in crate::storage::queries) enum Filter {
    Id(Id),
    Datagram {
        id: Id,
        epoch: tx::Epoch,
        server: net::SocketAddr,
    },
    Live {
        id: Id,
        now: time::Instant,
    },
    Stream(index::Server),
}

impl Filter {
    fn matches(self, probe: &state::Attempt<'_>, lease: &tx::Lease<'_>) -> bool {
        match self {
            Self::Id(want) => lease.id() == want,
            Self::Datagram { id, epoch, server } => {
                let tx::Lease::Udp {
                    removed,
                    epoch: issued,
                    ..
                } = lease
                else {
                    return false;
                };
                let req = probe.request();
                Id::from_ordinal(removed.index()) == id
                    && *issued == epoch
                    && req.stream().is_none()
                    && req.address() == server
            }
            Self::Live { id: want, now } => lease.id() == want && now < probe.deadline(),
            Self::Stream(server) => probe.request().stream() == Some(server),
        }
    }
}

pub(in crate::storage) struct Ledger<'s, const LANES: usize, const N: usize> {
    slots: Box<[Slot<'s, N>]>,
    pub(in crate::storage::queries) reg: Registry<'s>,
}

impl<'s, const LANES: usize, const N: usize> Ledger<'s, LANES, N> {
    pub(in crate::storage) fn try_new(
        layout: index::Layout,
        store: &'s storage::Store,
    ) -> Result<Self, storage::Error> {
        use crate::storage::queries::tx::Table;

        let mut slots = Vec::try_new(LANES).map_err(storage::Error::Allocation)?;
        for lane in 0..LANES {
            slots.push(
                Slot::try_new(store, index::Lane::from_raw(lane as u16))
                    .map_err(storage::Error::Entries)?,
            );
        }
        Ok(Self {
            slots: slots.into_boxed_slice(),
            reg: Registry {
                pending: Pending::new(&store.pending),
                live: stream::Live::new(),
                ids: Table::try_new(layout.transactions(), &store.ids)
                    .map_err(storage::Error::Allocation)?,
                stream_failure: None,
            },
        })
    }

    pub(in crate::storage::queries) fn start_udp(
        &mut self,
        now: time::Instant,
    ) -> Option<starts::Udp<'_, 's, N>> {
        let Self { slots, reg, .. } = self;
        let Registry {
            pending: wait,
            live,
            ids,
            ..
        } = reg;
        Some(starts::Udp::new(wait.claim_udp()?, slots, live, ids, now))
    }

    pub(in crate::storage::queries) fn start_tcp(
        &mut self,
        server: index::Server,
        id: Id,
        now: time::Instant,
    ) -> Option<starts::Tcp<'_, 's, N>> {
        let Self { slots, reg, .. } = self;
        let Registry {
            pending: wait,
            live,
            ids,
            ..
        } = reg;
        Some(starts::Tcp::new(
            wait.claim_stream(server)?,
            slots,
            live,
            ids,
            now,
            id,
        ))
    }

    pub(in crate::storage::queries) fn claim(
        &mut self,
        lane: index::Lane,
        family: family::Version,
        filter: Filter,
    ) -> Option<waiting::State<'_, 's, N>> {
        self.accept(lane, family, filter, |_, _| Some(()))
            .map(|(waiting, ())| waiting)
    }

    pub(in crate::storage::queries) fn accept<T>(
        &mut self,
        lane: index::Lane,
        family: family::Version,
        filter: Filter,
        decode: impl FnOnce(&state::Request<'s>, &tx::Lease<'s>) -> Option<T>,
    ) -> Option<(waiting::State<'_, 's, N>, T)> {
        let Self { slots, reg, .. } = self;
        let Slot::Query(query) = slots.get_mut(usize::from(lane))? else {
            return None;
        };
        let hard = query.hard_deadline;
        let slot = query.branch_mut(family);
        let state::Branch::Waiting {
            attempt: probe,
            transaction: lease,
        } = &slot.value
        else {
            return None;
        };
        if !filter.matches(probe, lease) {
            return None;
        }
        let decoded = decode(probe.request(), lease)?;
        let previous = mem::replace(&mut slot.value, state::Branch::Empty);
        match previous {
            state::Branch::Waiting {
                attempt: probe,
                transaction: lease,
            } => {
                reg.ids.release(lease);
                return Some((waiting::State::new(slot, reg, probe, hard), decoded));
            }
            previous => slot.value = previous,
        }
        None
    }

    pub(in crate::storage::queries) fn rollback(
        &mut self,
        lane: index::Lane,
        family: family::Version,
        id: Id,
    ) {
        if let Some(wait) = self.claim(lane, family, Filter::Id(id)) {
            drop(wait);
        }
    }

    pub(in crate::storage::queries) fn complete(&mut self, lane: index::Lane, now: time::Instant) {
        if let Some(slot) = self.slots.get_mut(usize::from(lane)) {
            slot.complete(now);
        }
    }

    pub(in crate::storage::queries) fn read<'a>(
        lane: Lane<&'a Self>,
    ) -> (&'a Slot<'s, N>, &'a Registry<'s>) {
        let (owner, lane) = lane.into_inner();
        // SAFETY: the proof retains this initialized lane in this exact immutable allocation.
        let slot = unsafe { owner.slots.get_unchecked(usize::from(lane)) };
        (slot, &owner.reg)
    }

    pub(in crate::storage::queries) fn write<'a>(
        lane: Lane<&'a mut Self>,
    ) -> (&'a mut Slot<'s, N>, &'a mut Registry<'s>) {
        let (owner, lane) = lane.into_inner();
        // SAFETY: the proof retains this initialized lane under the exclusive ledger loan.
        let slot = unsafe { owner.slots.get_unchecked_mut(usize::from(lane)) };
        (slot, &mut owner.reg)
    }

    pub(in crate::storage::queries) fn retire_lane(lane: Lane<&mut Self>) {
        let (slot, reg) = Self::write(lane);
        slot.retire(reg);
    }

    pub(in crate::storage::queries) fn start(lane: Lane<&mut Self>, seed: state::Seed<'_, 's>) {
        let (slot, reg) = Self::write(lane);
        slot.start(seed, reg);
    }
}

pub(in crate::storage::queries) struct Pending<'s> {
    sets: &'s set::State<index::Branch, u16, { config::MAX_SERVERS + 1 }>,
}

type Taken<'a, 's> = set::Entry<
    &'a mut Pending<'s>,
    index::Branch,
    u16,
    { config::MAX_SERVERS + 1 },
    set::Id<{ config::MAX_SERVERS + 1 }>,
    set::Planar,
>;

impl<'s> Pending<'s> {
    const SERVERS: [set::Id<{ config::MAX_SERVERS + 1 }>; config::MAX_SERVERS] = [
        set::Id::at::<1>(),
        set::Id::at::<2>(),
        set::Id::at::<3>(),
        set::Id::at::<4>(),
    ];

    pub(in crate::storage::queries) const fn new(
        sets: &'s set::State<index::Branch, u16, { config::MAX_SERVERS + 1 }>,
    ) -> Self {
        Self { sets }
    }

    pub(in crate::storage::queries) fn claim_udp(&mut self) -> Option<Claim<'_, 's>> {
        Some(Claim::new(set::Entry::take_front(
            self,
            set::Id::at::<0>(),
        )?))
    }

    pub(in crate::storage::queries) fn claim_stream(
        &mut self,
        server: index::Server,
    ) -> Option<Claim<'_, 's>> {
        Some(Claim::new(set::Entry::take_front(
            self,
            Self::id(Some(server)),
        )?))
    }

    pub(in crate::storage::queries) fn has_udp(&self) -> bool {
        !self.sets.get::<0>().is_empty()
    }

    pub(in crate::storage::queries) fn has_stream(&self) -> bool {
        index::Server::ALL
            .into_iter()
            .any(|server| !self.sets.plane(Self::id(Some(server))).is_empty())
    }

    pub(in crate::storage::queries) fn has_server(&self, server: index::Server) -> bool {
        !self.sets.plane(Self::id(Some(server))).is_empty()
    }

    pub(in crate::storage::queries) fn id(
        server: Option<index::Server>,
    ) -> set::Id<{ config::MAX_SERVERS + 1 }> {
        match server {
            Some(server) => *server.get(&Self::SERVERS),
            None => set::Id::at::<0>(),
        }
    }
}

#[must_use = "commit the selected request or restore it"]
pub(in crate::storage::queries) struct Claim<'a, 's> {
    taken: Lease<Restore<'a, 's>, Open>,
}

struct Restore<'a, 's>(Taken<'a, 's>);

impl<'a, 's> Claim<'a, 's> {
    pub(in crate::storage::queries::ledger) fn new(taken: Taken<'a, 's>) -> Self {
        Self {
            taken: Lease::open(Restore(taken), ()),
        }
    }

    pub(in crate::storage::queries::ledger) fn commit(self) {
        let (Restore(_), ()) = self.taken.into_parts();
    }
}

impl Return for Restore<'_, '_> {
    type Item = ();

    fn return_item(self, (): Self::Item) {
        self.0.restore();
    }
}

impl<'a, 's> ops::Deref for Claim<'a, 's> {
    type Target = Taken<'a, 's>;
    fn deref(&self) -> &Self::Target {
        &self.taken.sink().0
    }
}

const _: () = {
    assert!(
        mem::size_of::<Claim<'static, 'static>>() == mem::size_of::<Restore<'static, 'static>>()
    );
    assert!(
        mem::size_of::<Option<Claim<'static, 'static>>>()
            == mem::size_of::<Option<Restore<'static, 'static>>>()
    );
};

// SAFETY: the exclusive controller retains its original immutable storage loan.
unsafe impl depot::raw::SetSource<index::Branch, u16, { config::MAX_SERVERS + 1 }>
    for &mut Pending<'_>
{
    fn planes(&self) -> &set::State<index::Branch, u16, { config::MAX_SERVERS + 1 }> {
        self.sets
    }
}
