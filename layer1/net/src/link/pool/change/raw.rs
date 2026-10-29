use std::marker;

use dope_core::driver::{
    self,
    retained::flight,
    route::{Identity, lease, table::conn},
};
use o3::buffer::storage::Shared;

use crate::{
    link::{
        egress::{self, reserve::demand},
        pool::{
            self, change, pending,
            recv::{self, input, storage::tail},
        },
        slot::types,
    },
    wire::{self, domain},
};

/// The borrowed backing shared by one cold pool's connection lanes.
///
/// Connection state and its wire runtime enter after this storage scope exists.
/// Consumption retains the same egress owner, pending queue and flight proof.
pub struct Storage<'p, 'd, ID: Identity, B, const IOV: usize, D: demand::Mode> {
    capacity: conn::Capacity,
    flights: flight::Need<ID>,
    egress: egress::Storage<'p, 'd, ID, B, IOV, D>,
    pending: &'p pending::Queue<'d, ID>,
}

impl<'d, ID: Identity, B, const IOV: usize, D: demand::Mode> Storage<'_, 'd, ID, B, IOV, D> {
    /// Opens the existing egress and pending-queue loans before application creation.
    pub fn try_with<C, O>(
        setup: demand::Setup<D>,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        build: impl for<'p> FnOnce(Storage<'p, 'd, ID, B, IOV, D>, &mut C) -> O,
    ) -> crate::Result<O> {
        let cap = setup.capacity();
        let capacity = flight::Capacity::connections::<2>(cap)
            .map_err(crate::ConfigError::FlightCapacity)
            .map_err(crate::Resource::Config)?;
        let pending = pending::Queue::try_new(cap).map_err(crate::Resource::Allocation)?;
        let mut owner = (owner, pending);
        egress::Storage::try_with(
            setup,
            &mut owner,
            |owner| context(owner.0),
            |egress, (owner, pending)| {
                build(
                    Storage {
                        capacity: cap,
                        flights: flight::Need::new(capacity),
                        egress,
                        pending,
                    },
                    owner,
                )
            },
        )
        .map_err(crate::Resource::Egress)
    }
}

impl<'p, 'd, ID: Identity, B, const IOV: usize, D: demand::Mode> Storage<'p, 'd, ID, B, IOV, D> {
    /// Initializes owned connection state inside the established storage loan.
    pub fn prepare<'w, T, W, S, M, R>(
        self,
        retain: domain::Retained,
        bytes: domain::Resident,
        end: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> crate::Result<Admission<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>>
    where
        'd: 'w,
        ID: 'w,
        T: crate::Transport,
        W: wire::Spec + 'w + 'd,
        M: input::Mode<W>,
        R: change::Prepare<'w, 'd, ID, W>,
    {
        use crate::link::pool::recv::storage::{raw::Mode, tail::Store};

        let cap = self.capacity;
        let slab =
            input::Tail::<'w, 'd, W, M>::try_new::<types::Connection<'w, 'd, ID, W, S>, ID>(cap)?;
        let receive = driver.receive();
        let limits = domain::Limits::new(cap, retain, bytes, receive.buffer_len());
        let runtime = end.try_runtime(limits)?;
        let recv = recv::State::try_new(
            cap,
            M::Ingress::try_storage(cap, receive.buffers(), driver.driver_ref())?,
        )?;
        let slab = slab.try_storage::<M::Ingress>(recv)?;
        Ok((
            Prepared {
                flights: (),
                slab,
                egress: self.egress,
                runtime,
                pending: self.pending,
                policy: marker::PhantomData,
            },
            self.flights,
        ))
    }
}

/// Fully allocated storage which cannot submit until a route is moved into it.
pub struct Prepared<
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B = Shared,
    const IOV: usize = 32,
    D: demand::Mode = demand::Fallible,
    F = flight::Slots<'d, ID>,
> where
    'd: 'w,
{
    pub(in crate::link::pool) flights: F,
    pub(in crate::link::pool) slab: Resident<'w, 'd, ID, W, S, M>,
    pub(in crate::link::pool) egress: egress::Storage<'p, 'd, ID, B, IOV, D>,
    pub(in crate::link::pool) runtime: W::Runtime<'w, 'd, ID>,
    pub(in crate::link::pool) pending: &'p pending::Queue<'d, ID>,
    pub(in crate::link::pool) policy: marker::PhantomData<(T, M)>,
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D, ()>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    'd: 'w,
{
    pub fn commit(
        self,
        flights: flight::Slots<'d, ID>,
    ) -> Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        let Self {
            flights: (),
            slab,
            egress,
            runtime,
            pending,
            policy,
        } = self;
        Prepared {
            flights,
            slab,
            egress,
            runtime,
            pending,
            policy,
        }
    }
}

impl<'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
    'd: 'w,
{
    /// # Safety
    /// Install the owning lifecycle before submission, then retain through shutdown and finish.
    /// Before installation, dropping is valid only while no operation has been submitted.
    pub unsafe fn bind(
        self,
        route: lease::Claim<'d, ID>,
    ) -> pool::Pool<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D> {
        let keys = pool::Keyspace::from_claim(&route);
        pool::Pool {
            storage: pool::Connections {
                keys: route.store(keys),
                prepared: self,
            },
        }
    }
}

type Resident<'w, 'd, ID, W, S, M> = tail::Table<
    types::Connection<'w, 'd, ID, W, S>,
    ID,
    input::Tail<'w, 'd, W, M>,
    recv::State<'d, ID, <M as input::Mode<W>>::Ingress>,
>;
type Unbound<'p, 'w, 'd, ID, T, W, S, M, B, const IOV: usize, D> =
    Prepared<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D, ()>;

pub(in crate::link::pool) type Admission<'p, 'w, 'd, ID, T, W, S, M, B, const IOV: usize, D> = (
    Unbound<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    flight::Need<ID>,
);
