mod accept;
pub mod config;
pub mod connection;
pub mod handler;
pub mod raw;
mod runtime;
mod sealed;
mod writer;

use std::{io, marker, net, pin, time};

use dope_core::driver::{
    self, ops,
    route::{self, kind, table},
};
use dope_net::{
    link::{egress::data, pool, pool::pending},
    tcp, wire,
};
use dope_runtime::random;
use o3::{
    cell::region,
    collections::{self, queue::slot},
};
pub(in crate::listener) use sealed::{Binding, IOV_CAP};

use crate::{dispatch::typed, timing};

const HASH_DOMAIN_BASE: u64 = u64::from_be_bytes(*b"\0\0accept");

/// Accept-table hash domains separated by listener identity.
pub enum Domain {}

impl Domain {
    /// Accept-table hash domain for listener zero.
    pub const DEFAULT: random::Domain = Self::for_listener(0);

    pub const fn for_listener(listener_id: u8) -> random::Domain {
        random::Domain::new(HASH_DOMAIN_BASE ^ listener_id as u64)
    }
}

pub(in crate::listener) struct Inbound;
pub(in crate::listener) struct SendDeadline;
pub(in crate::listener) struct Absolute;

#[derive(Clone, Copy)]
struct DeadlineEntry<'d, const ID: u8> {
    at: time::Instant,
    key: pool::Key<'d, ID>,
}

pub(in crate::listener) struct Deadline<'d, const ID: u8, K> {
    queue: slot::Fifo<DeadlineEntry<'d, ID>>,
    window: time::Duration,
    capacity: table::Capacity,
    kind: marker::PhantomData<fn() -> K>,
}

impl<'d, const ID: u8, K> Deadline<'d, ID, K> {
    fn try_new(
        capacity: table::Capacity,
        window: time::Duration,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            queue: slot::Fifo::try_with_capacity(capacity.get())?,
            window,
            capacity,
            kind: marker::PhantomData,
        })
    }

    fn arm(&mut self, key: pool::Key<'d, ID>, now: time::Instant) -> bool {
        let lane = key.index();
        if self.capacity.slot(lane).is_none() {
            return false;
        }
        let Some(deadline) = now.checked_add(self.window) else {
            return false;
        };
        self.queue
            .refresh_back(lane, DeadlineEntry { at: deadline, key })
            .is_ok()
    }

    fn cancel(&mut self, key: pool::Key<'d, ID>) {
        let lane = key.index();
        self.queue.remove_if(lane, |entry| entry.key == key);
    }

    fn pop_expired(&mut self, now: time::Instant) -> Option<pool::Key<'d, ID>> {
        let (_, &DeadlineEntry { at, key }) = self.queue.front_key_value()?;
        if at > now {
            return None;
        }
        self.queue.pop_front();
        Some(key)
    }

    fn earliest(&self) -> Option<time::Instant> {
        self.queue.front_key_value().map(|(_, entry)| entry.at)
    }
}

pub(in crate::listener) trait DeadlineKind<'d, const ID: u8>: Sized {
    fn get<'a>(schedule: &'a mut Schedule<'d, ID>) -> &'a mut Deadline<'d, ID, Self>;
}

impl<'d, const ID: u8> DeadlineKind<'d, ID> for Inbound {
    fn get<'a>(schedule: &'a mut Schedule<'d, ID>) -> &'a mut Deadline<'d, ID, Self> {
        &mut schedule.inbound
    }
}

impl<'d, const ID: u8> DeadlineKind<'d, ID> for SendDeadline {
    fn get<'a>(schedule: &'a mut Schedule<'d, ID>) -> &'a mut Deadline<'d, ID, Self> {
        &mut schedule.send
    }
}

impl<'d, const ID: u8> DeadlineKind<'d, ID> for Absolute {
    fn get<'a>(schedule: &'a mut Schedule<'d, ID>) -> &'a mut Deadline<'d, ID, Self> {
        &mut schedule.absolute
    }
}

enum Shutdown {
    Open,
    Closing(ShutdownCursor),
    Done,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct ShutdownCursor(usize);

impl ShutdownCursor {
    fn take(&mut self, capacity: table::Capacity) -> Option<route::SlotIndex> {
        let slot = capacity.slot(self.0)?;
        self.0 += 1;
        Some(slot)
    }
}

#[derive(Clone, Copy)]
pub(in crate::listener) enum Phase {
    Accept,
    Ingress,
    Dirty,
    Inbound,
    Send,
    Absolute,
}

impl Phase {
    pub(in crate::listener) const COUNT: usize = 6;

    const fn next(self) -> Self {
        match self {
            Self::Accept => Self::Ingress,
            Self::Ingress => Self::Dirty,
            Self::Dirty => Self::Inbound,
            Self::Inbound => Self::Send,
            Self::Send => Self::Absolute,
            Self::Absolute => Self::Accept,
        }
    }
}

pub(in crate::listener) struct Schedule<'d, const ID: u8> {
    inbound: Deadline<'d, ID, Inbound>,
    send: Deadline<'d, ID, SendDeadline>,
    absolute: Deadline<'d, ID, Absolute>,
    shutdown: Shutdown,
    phase: Phase,
}

impl<'d, const ID: u8> Schedule<'d, ID> {
    fn try_new<P: timing::Policy>(
        capacity: table::Capacity,
    ) -> Result<Self, collections::AllocationError> {
        Ok(Self {
            inbound: Deadline::try_new(capacity, P::IDLE_WINDOW.get())?,
            send: Deadline::try_new(capacity, P::SEND_DEADLINE.get())?,
            absolute: Deadline::try_new(capacity, P::ABS_CONN_AGE.get())?,
            shutdown: Shutdown::Open,
            phase: Phase::Accept,
        })
    }

    fn earliest(&self) -> Option<time::Instant> {
        [
            self.inbound.earliest(),
            self.send.earliest(),
            self.absolute.earliest(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn begin_shutdown(&mut self) {
        if matches!(self.shutdown, Shutdown::Open) {
            self.shutdown = Shutdown::Closing(ShutdownCursor(0));
        }
    }

    fn take_shutdown(&mut self, capacity: table::Capacity) -> Option<route::SlotIndex> {
        let Shutdown::Closing(cursor) = &mut self.shutdown else {
            return None;
        };
        let next = cursor.take(capacity);
        if next.is_none() {
            self.shutdown = Shutdown::Done;
        }
        next
    }

    fn is_closing(&self) -> bool {
        matches!(self.shutdown, Shutdown::Closing(_))
    }

    fn is_done(&self) -> bool {
        matches!(self.shutdown, Shutdown::Done)
    }

    fn next_phase(&mut self) -> Phase {
        let phase = self.phase;
        self.phase = phase.next();
        phase
    }
}
#[pin_project::pin_project(!Unpin)]
pub struct Listener<
    'd,
    const ID: u8,
    A,
    E = crate::Bundle<tcp::Tcp, <A as handler::Application<'d, ID>>::Wire, timing::Balanced>,
> where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    owner: writer::Owner<'d, ID, E::Transport, A::Wire, A::Conn, A::Input>,
    #[pin]
    app: A,
    #[pin]
    accept: accept::Accept<'d, ID>,
    bound_addr: net::SocketAddr,
    schedule: Schedule<'d, ID>,
}

/// Lifecycle-preserving listener operations for one application step.
pub struct Control<'step, 'd, const ID: u8, A, E>
where
    'd: 'step,
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    inner: pin::Pin<&'step mut Listener<'d, ID, A, E>>,
}

impl<'step, 'd, const ID: u8, A, E> Control<'step, 'd, ID, A, E>
where
    'd: 'step,
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    pub fn handler_pin(&self) -> pin::Pin<&A> {
        self.inner.as_ref().project_ref().app
    }

    pub fn handler_control(&mut self) -> <A as raw::ControlHandler<'d, ID>>::Control<'_>
    where
        A: raw::ControlHandler<'d, ID>,
    {
        let installed = raw::Installed::new(self);
        installed.control()
    }

    pub fn set_close_after(&mut self, conn_id: connection::Id<'d, ID>) {
        let this = self.inner.as_mut().project();
        if let Some(slot) = this.owner.pool_mut().get_mut(conn_id.key()) {
            slot.set_close_after();
        }
    }

    pub fn close(&self, conn_id: connection::Id<'d, ID>) {
        self.inner.as_ref().get_ref().close(conn_id);
    }

    pub fn mark_send(
        &self,
        region: &mut region::Token<'d>,
        conn_id: connection::Id<'d, ID>,
        bytes: data::Buffer<'d>,
    ) -> bool {
        self.inner
            .as_ref()
            .get_ref()
            .mark_send(region, conn_id, bytes)
    }
}

impl<'d, const ID: u8, A, E> Listener<'d, ID, A, E>
where
    A: handler::Application<'d, ID>,
    E: crate::Env<Wire = A::Wire>,
{
    pub fn open_in(
        app: A,
        config: config::Config<E::Transport>,
        hash_builder: random::HashState<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self>
    where
        E::Transport: dope_net::ListenerTransport,
        <A::Wire as wire::Wire>::InitConfig<'d, ID>: Default,
    {
        Self::open_in_with_wire(app, config, Default::default(), hash_builder, driver)
    }

    pub fn open_in_with_wire(
        app: A,
        config: config::Config<E::Transport>,
        wire_config: <A::Wire as wire::Wire>::InitConfig<'d, ID>,
        hash_builder: random::HashState<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self>
    where
        E::Transport: dope_net::ListenerTransport,
    {
        let listener = Self::assemble(app, config, wire_config, hash_builder, driver)?;
        Ok(listener)
    }

    fn assemble(
        app: A,
        config: config::Config<E::Transport>,
        wire_config: <A::Wire as wire::Wire>::InitConfig<'d, ID>,
        hash_builder: random::HashState<'d>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self>
    where
        E::Transport: dope_net::ListenerTransport,
    {
        use std::io::{Error, ErrorKind};

        use dope_core::driver::{lifecycle::routing::Route, route::table::ConnectionCapacity};
        use dope_net::{ListenerTransport, Transport, link::pool::Prepared};

        let config::Config {
            max_connections,
            direct_flights,
            bind,
            backlog,
            stream,
            transport,
            egress: egress_config,
        } = config;
        let Some(connection_capacity) = ConnectionCapacity::new(max_connections) else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: listener capacity must be in 1..=2^24-1",
            ));
        };
        let max_connections = connection_capacity.get();
        if direct_flights > max_connections {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: listener write-flight capacity exceeds connection capacity",
            ));
        }
        if max_connections > ops::Buffers::accept_capacity(driver) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: listener capacity exceeds driver accept slots",
            ));
        }
        let options = <E::Transport as Transport>::stream_options(stream)?;
        let capacity = connection_capacity.table();
        let accept_slot = connection_capacity.sentinel();
        let max_retained_recv_chunks =
            <A::Input as handler::Policy<'d, ID, A>>::retained_capacity(max_connections)?;
        let per_ip_limit = <E::Transport as ListenerTransport>::per_ip_limit(&transport)
            .unwrap_or(<E::Admission as config::Admission>::PER_IP_LIMIT);
        let accept = accept::Prepared::try_new(max_connections, per_ip_limit, hash_builder)?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "dope: listener capacity exceeds peer table layout",
                )
            })?;
        let prepared_pool = Prepared::new(
            capacity,
            max_retained_recv_chunks,
            egress_config,
            wire_config,
            driver,
        )?;
        let writes = writer::Prepared::try_new(
            driver.region_token_ref(),
            egress_config,
            direct_flights as u32,
        )?;
        let schedule = Schedule::try_new::<E::Timing>(capacity)?;
        let accept_flights =
            driver.flight_slots::<route::KeyTag<ID, { kind::ACCEPT }>>(accept.flight_capacity())?;
        let mut route = Route::reserve_transaction(driver)?;
        let (listener_fd, bound_addr) = <E::Transport as ListenerTransport>::bind_listener_slot(
            route.driver(),
            &bind,
            backlog,
            &transport,
        )?;
        let accept = accept.bind(listener_fd, accept_flights, accept_slot, options)?;
        let route = route.commit();
        let pool = Binding::bind(prepared_pool, route);
        Ok(Self {
            owner: writer::Owner::new(pool, writes),
            accept,
            bound_addr,
            app,
            schedule,
        })
    }

    pub const fn local_addr(&self) -> net::SocketAddr {
        self.bound_addr
    }

    pub fn handler(&self) -> &A {
        &self.app
    }

    pub fn capacity(&self) -> usize {
        self.owner.pool().inspection().capacity().get()
    }

    /// Converts an activation target for this exact listener into its
    /// lifetime- and route-branded connection identity.
    pub fn connection_id(&self, target: typed::Token<'d, Self>) -> Option<connection::Id<'d, ID>> {
        self.owner
            .pool()
            .by_target(target.raw())
            .map(|(key, _)| connection::Id::from_key(key))
    }

    /// Rebrands a connection identity as this exact listener's activation target.
    ///
    /// Activation and any requested operation perform their own liveness check.
    #[doc(hidden)]
    pub const fn activation_target(
        &self,
        connection: connection::Id<'d, ID>,
    ) -> typed::Token<'d, Self> {
        let key = connection.key();
        typed::Token(
            route::Token::new(ID, key.lane(), key.epoch()),
            marker::PhantomData,
        )
    }

    pub fn mark_send(
        &self,
        region: &mut region::Token<'d>,
        conn_id: connection::Id<'d, ID>,
        bytes: data::Buffer<'d>,
    ) -> bool {
        self.owner
            .pool()
            .try_stage(region, conn_id.key(), writer::Payload::Body(bytes))
            .is_ok()
    }

    pub fn close(&self, conn_id: connection::Id<'d, ID>) {
        let Some((_, handle)) = pending::Pending::of(self.owner.pool()).get(conn_id.key()) else {
            return;
        };
        handle.mark(pending::Action::Close);
    }
}
