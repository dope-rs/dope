use std::{io, marker, mem, ops};

use dope_core::{
    driver::{
        self, flight,
        lifecycle::{self, routing},
        ops::{Buffers as _, Files as _},
        retained,
        route::{self, table, table::entries::vacant},
    },
    io::{fd::handles, socket::option},
};
use o3::{buffer::storage, cell::region};

use self::ingress::{acceptance, recvs};
use crate::{
    link::{
        self,
        egress::{self, data},
        slot::types,
    },
    wire::{self, receive, send},
};

pub mod ingress;
pub mod input;
mod inspection;
mod key;
mod outbound;
pub mod pending;
#[doc(hidden)]
pub mod raw;
pub mod transition;

pub use inspection::Inspection;
pub use key::Key;
pub use outbound::Outbound;
pub(in crate::link) use outbound::StoredAddress;

#[derive(Clone, Copy)]
pub(in crate::link) struct Keyspace<'d, const ID: u8> {
    space: route::Space<'d, route::KeyTag<ID>>,
}

const _: () = assert!(mem::size_of::<Keyspace<'static, 0>>() == 0);

impl<'d, const ID: u8> Keyspace<'d, ID> {
    fn from_route(route: &routing::Route<'d, ID>) -> Self {
        Self {
            space: route::Space::for_driver(route.driver()),
        }
    }

    pub(in crate::link) const fn parse(self, target: route::Token) -> Option<Key<'d, ID>> {
        match self.space.parse(target) {
            Some(target) => Some(Key::from_target(target)),
            None => None,
        }
    }

    pub(in crate::link) const fn bind_table(
        self,
        key: table::Key<route::KeyTag<ID>>,
    ) -> Key<'d, ID> {
        Key::from_target(self.space.bind_key(key))
    }

    pub(in crate::link) const fn bind_parts(
        self,
        slot: route::SlotIndex,
        epoch: route::Epoch,
    ) -> Key<'d, ID> {
        Key::from_target(
            self.space
                .bind_parts(table::Parts::from_components(slot, epoch)),
        )
    }
}

/// Connection storage which exclusively owns the route of every retained target.
#[doc(hidden)]
pub struct Connections<
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B = storage::Shared,
    const IOV: usize = 32,
> {
    route: routing::Route<'d, ID>,
    keys: Keyspace<'d, ID>,
    prepared: Prepared<'d, ID, T, W, S, M, B, IOV>,
}

pub struct Pool<
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B = storage::Shared,
    const IOV: usize = 32,
> {
    storage: Connections<'d, ID, T, W, S, M, B, IOV>,
}

/// Exclusive connection and egress projection backed by one pool borrow.
pub struct EgressMut<'a, 'd, const ID: u8, W: wire::Wire, S, B, const IOV: usize> {
    pub flights: &'a flight::Slots<'d, route::KeyTag<ID>>,
    pub connection: &'a mut types::Connection<'d, ID, W, S>,
    pub queue: egress::Queue<'a, 'd, IOV, B>,
}

impl<const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> Drop
    for Connections<'_, ID, T, W, S, M, B, IOV>
{
    fn drop(&mut self) {
        self.route.assert_droppable();
    }
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> ops::Deref
    for Pool<'d, ID, T, W, S, M, B, IOV>
{
    type Target = Connections<'d, ID, T, W, S, M, B, IOV>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize> ops::DerefMut
    for Pool<'_, ID, T, W, S, M, B, IOV>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

struct Scheduling<'d, const ID: u8> {
    rearm: link::Rearm<'d, ID>,
    pending: pending::Queue<'d, ID>,
}

impl<'d, const ID: u8> Scheduling<'d, ID> {
    fn reserve<'a, T>(
        &'a self,
        keys: Keyspace<'d, ID>,
        reservation: vacant::Entry<'a, T, route::KeyTag<ID>>,
    ) -> pending::Vacancy<'a, 'd, ID, T> {
        pending::Vacancy::new(&self.pending, &self.rearm, keys, reservation)
    }
}

#[doc(hidden)]
/// Fully allocated storage which cannot submit until a route is moved into it.
pub struct Prepared<
    'd,
    const ID: u8,
    T: crate::Transport,
    W: wire::Wire,
    S,
    M,
    B = storage::Shared,
    const IOV: usize = 32,
> {
    flights: flight::Slots<'d, route::KeyTag<ID>>,
    slab: table::Slab<types::Stored<'d, ID, W, S, IOV>, route::KeyTag<ID>>,
    egress: egress::Storage<'d, B, IOV>,
    runtime: W::RuntimeContext<'d, ID>,
    scheduling: Scheduling<'d, ID>,
    deferred_recv: recvs::Recvs<'d>,
    _t: marker::PhantomData<(T, M)>,
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Prepared<'d, ID, T, W, S, M, B, IOV>
where
    M: input::Mode<W>,
{
    pub fn new(
        capacity: table::Capacity,
        max_retained_recv_chunks: usize,
        egress_config: egress::Config,
        wire_config: W::InitConfig<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        use crate::wire::{RuntimeLimits, batch::raw::Source};

        let recv_batch_min = <<W as wire::Wire>::RecvBatch<'static> as Source>::MIN_CAPACITY.get();
        let recv_batch_limit = <<W as wire::Wire>::RecvBatch<'static> as Source>::MAX_ITEMS.get();
        if recv_batch_min > recv_batch_limit || recv_batch_limit > wire::MAX_RECV_BATCH_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: wire receive batch must be in 1..=32",
            ));
        }
        let max_connections = capacity.get();
        let flight_capacity = max_connections.checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: connection flight capacity overflow",
            )
        })?;
        let flights = driver.flight_slots::<route::KeyTag<ID>>(flight_capacity)?;
        let (deferred_connections, deferred_recv_slots) =
            <M as input::Mode<W>>::deferred_capacity(max_connections, driver.buffer_count())?;
        let slab = table::Slab::try_with_capacity(capacity)?;
        let egress = egress::Storage::try_with_config(
            driver.region_token_ref(),
            egress_config,
            max_connections,
        )?;
        let limits = RuntimeLimits::new(
            max_connections,
            max_retained_recv_chunks,
            driver.buffer_len(),
        );
        let runtime = W::runtime_context::<ID>(limits, wire_config)?;
        Ok(Self {
            flights,
            slab,
            egress,
            runtime,
            scheduling: Scheduling {
                rearm: link::Rearm::try_with_capacity(capacity)?,
                pending: pending::Queue::try_with_capacity(capacity)?,
            },
            deferred_recv: recvs::Recvs::try_with_capacity(
                deferred_connections,
                deferred_recv_slots,
            )?,
            _t: marker::PhantomData,
        })
    }
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Connections<'d, ID, T, W, S, M, B, IOV>
{
    #[doc(hidden)]
    pub fn install(&self, install: &mut lifecycle::Install<'_, 'd>) {
        self.route.install(install);
    }

    pub fn ingress(&mut self) -> ingress::Ingress<'_, 'd, ID, T, W, S, M, B, IOV>
    where
        M: input::Mode<W>,
    {
        ingress::Ingress::new(self)
    }

    pub fn inspection(&self) -> Inspection<'_, 'd, ID, T, W, S, M, B, IOV> {
        Inspection::new(self)
    }

    #[doc(hidden)]
    pub fn driver(&self) -> driver::Reference<'d> {
        self.route.driver()
    }

    pub fn get(&self, key: Key<'d, ID>) -> Option<&types::Connection<'d, ID, W, S>> {
        self.prepared
            .slab
            .entries()
            .at_parts(key.parts())
            .map(|slot| &slot.connection)
    }

    pub fn get_mut(&mut self, key: Key<'d, ID>) -> Option<&mut types::Connection<'d, ID, W, S>> {
        self.prepared
            .slab
            .entries_mut()
            .at_parts(key.parts())
            .map(|slot| &mut slot.connection)
    }

    pub fn egress_mut(&mut self, key: Key<'d, ID>) -> Option<EgressMut<'_, 'd, ID, W, S, B, IOV>>
    where
        B: data::Payload<'d>,
    {
        let Prepared {
            flights,
            slab,
            egress,
            ..
        } = &mut self.prepared;
        let slot = slab.entries_mut().at_parts(key.parts())?;
        let queue = egress.queue(&mut slot.egress);
        Some(EgressMut {
            flights,
            connection: &mut slot.connection,
            queue,
        })
    }

    pub fn try_stage(
        &self,
        token: &mut region::Token<'d>,
        key: Key<'d, ID>,
        bytes: B,
    ) -> Result<(), B>
    where
        B: data::Payload<'d>,
    {
        let Some(slot) = self.prepared.slab.entries().at_parts(key.parts()) else {
            return Err(bytes);
        };
        let Some(handle) = self.prepared.scheduling.pending.handle(key) else {
            return Err(bytes);
        };
        let staged = self.prepared.egress.try_enqueue(token, &slot.egress, bytes);
        handle.mark(pending::Action::Egress);
        staged
    }

    pub fn egress(&self, key: Key<'d, ID>) -> Option<(&types::Connection<'d, ID, W, S>, usize)>
    where
        B: data::Payload<'d>,
    {
        let slot = self.prepared.slab.entries().at_parts(key.parts())?;
        Some((&slot.connection, slot.egress.metadata.bytes()))
    }

    pub fn by_target(
        &self,
        target: route::Token,
    ) -> Option<(Key<'d, ID>, &types::Connection<'d, ID, W, S>)> {
        let key = self.keys.parse(target)?;
        self.prepared
            .slab
            .entries()
            .at_parts(key.parts())
            .map(|slot| (key, &slot.connection))
    }

    pub fn by_target_mut(
        &mut self,
        target: route::Token,
    ) -> Option<(Key<'d, ID>, &mut types::Connection<'d, ID, W, S>)> {
        let key = self.keys.parse(target)?;
        self.prepared
            .slab
            .entries_mut()
            .at_parts(key.parts())
            .map(|slot| (key, &mut slot.connection))
    }

    pub(in crate::link) fn by_target_submit_mut(
        &mut self,
        target: route::Token,
    ) -> Option<(
        &flight::Slots<'d, route::KeyTag<ID>>,
        Key<'d, ID>,
        &mut types::Connection<'d, ID, W, S>,
    )> {
        let key = self.keys.parse(target)?;
        let Prepared { flights, slab, .. } = &mut self.prepared;
        slab.entries_mut()
            .at_parts(key.parts())
            .map(|slot| (&*flights, key, &mut slot.connection))
    }

    pub fn refresh_wake(&self, key: Key<'d, ID>) {
        let Some(slot) = self.get(key) else {
            return;
        };
        slot.engine
            .ready_handle()
            .set_target(key.target().dispatch());
    }

    #[doc(hidden)]
    pub fn key_at(&self, lane: route::SlotIndex) -> Option<Key<'d, ID>> {
        let keys = self.keys;
        self.prepared
            .slab
            .entries()
            .current(lane)
            .map(|(_, key)| keys.bind_table(key))
    }

    fn close_removed(
        &mut self,
        slot: types::Stored<'d, ID, W, S, IOV>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let slot = slot.into_connection();
        slot.engine.close(driver);
        let availability = send::StorageBackend::release(slot.send);
        if <W::Receive as receive::Strategy<W>>::BACKPRESSURE && availability.is_released() {
            <W::Receive as receive::Strategy<W>>::send_released(&mut self.prepared.runtime);
        }
    }

    fn submit_recv(
        slot: &mut types::Connection<'d, ID, W, S>,
        flights: &flight::Slots<'d, route::KeyTag<ID>>,
        target: route::Target<'d, route::KeyTag<ID>>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> bool {
        if slot.engine.recv.is_armed() {
            return true;
        }
        if slot.engine.recv.is_paused() {
            return false;
        }
        let flight = slot
            .engine
            .fd()
            .and_then(|fd| link::Receive::new(fd, target).submit(flights, driver).ok());
        let armed = flight.is_some();
        slot.engine.recv.armed(&mut slot.engine.flights, flight);
        armed
    }
}

impl<'d, const ID: u8, T: crate::Transport, W: wire::Wire, S, M, B, const IOV: usize>
    Pool<'d, ID, T, W, S, M, B, IOV>
{
    #[doc(hidden)]
    pub fn accept_with<F, R>(
        &mut self,
        index: route::SlotIndex,
        fd: handles::Descriptor<'d>,
        options: option::StreamOptions,
        build_state: F,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<acceptance::Outcome<'d, ID, R>, W::OpenError>
    where
        F: FnOnce() -> Result<S, R>,
    {
        let accepted = link::Engine::accepted(fd);
        let keys = self.storage.keys;
        let Some(reservation) = self.storage.prepared.slab.vacant_entry_at(index.raw()) else {
            driver.close(accepted.into_fd());
            return Ok(acceptance::Outcome::Unavailable);
        };
        let reservation = self.storage.prepared.scheduling.reserve(keys, reservation);
        let Some(lane) = self.storage.prepared.egress.lane(index.raw() as usize) else {
            driver.close(accepted.into_fd());
            return Ok(acceptance::Outcome::Unavailable);
        };
        let open = match W::prepare_open::<ID>(&mut self.storage.prepared.runtime) {
            Ok(Some(open)) => open,
            Ok(None) => {
                driver.close(accepted.into_fd());
                return Ok(acceptance::Outcome::Unavailable);
            }
            Err(error) => {
                driver.close(accepted.into_fd());
                return Err(error);
            }
        };
        let state = match build_state() {
            Ok(state) => state,
            Err(reason) => {
                driver.close(accepted.into_fd());
                return Ok(acceptance::Outcome::Rejected(reason));
            }
        };
        let (wire, send) = wire::OpenReservation::commit(open);
        let outcome = reservation.commit_with(|key| {
            use crate::link::slot::types::Connection;

            let (engine, tuning) = accepted.tune(driver, options, key.target());
            let outcome = match tuning {
                link::AcceptedTuning::Ready => acceptance::Outcome::Ready(key),
                link::AcceptedTuning::Pending => acceptance::Outcome::Pending,
                link::AcceptedTuning::Failed => acceptance::Outcome::Failed(key),
            };
            let slot = types::Stored::new(Connection::new(engine, wire, send, key, state), lane);
            (slot, outcome)
        });
        Ok(outcome)
    }

    #[doc(hidden)]
    pub fn tuning(&mut self) -> acceptance::Tuning<'_, 'd, ID, T, W, S, M, B, IOV> {
        acceptance::Tuning::new(&mut self.storage)
    }

    #[doc(hidden)]
    pub fn finish(&mut self, finish: &mut lifecycle::Finalize<'_, 'd>) {
        assert!(
            self.storage.prepared.slab.is_empty(),
            "retained connection owner reached finish before quiescence"
        );
        finish.retire_route(&self.storage.route);
    }
}
