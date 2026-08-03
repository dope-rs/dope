pub mod application;
pub mod config;
pub mod egress;
mod idle;
mod raw;
pub mod recv;
mod send;
pub mod state;

use std::io::{self, Error, ErrorKind};
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::process::abort;
use std::time::Duration;

use application::{Application, ApplicationHooks, ApplicationPhase};
use config::Config;
use dope_core::driver::OutboundReservation;
use dope_core::driver::route::Route;
use dope_core::driver::token::{Token, TokenCapacity};
use dope_core::io::Event;
use dope_net::link;
use dope_net::link::egress::arena::Arena;
use dope_net::link::raw::pool::Pool;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PendingQueue, SendBuffer, Slot};
use dope_net::tcp::Tcp;
use dope_net::wire::Wire;
use dope_net::{ListenerTransport, Transport};
use egress::EgressPhase;
use idle::{IdlePhase, IdleSet};
use o3::buffer::Shared;
use pin_project::{pin_project, pinned_drop};
use raw::accept::{Accept, AcceptPhase};
use send::SendPhase;
use state::{Aux, EgressCtx, State};

use crate::manifold::Manifold;
use crate::manifold::env::{Bundle, Env};
use crate::manifold::typed::TypedToken;
use crate::panic::abort_on_drop_panic;
use crate::runtime::dispatcher::{FinishContext, Idle};
use crate::runtime::profile::{Balanced, RuntimeProfile};
use crate::{DriverContext, hash};

#[pin_project(PinnedDrop, !Unpin)]
pub struct Listener<
    'pool,
    'd,
    const ID: u8,
    A,
    E = Bundle<Tcp, <A as Application<'d>>::Wire, Balanced>,
> where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    route: Route<'d, ID>,
    pool: Pool<'d, ID, E::Transport, A::Wire, State<A::Conn>>,
    egress_arena: Arena<'d, 'pool, SendBuffer>,
    #[pin]
    app: A,
    aux: Aux,
    accept: Accept<'d, E::Transport>,
    bound_addr: SocketAddr,
    idle: IdleSet,
    idle_send: IdleSet,
    idle_abs_age: IdleSet,
    dirty: PendingQueue,
}

#[cold]
fn teardown_slot<'d, A>(
    mut app: Pin<&mut A>,
    slot: &mut Slot<'d, A::Wire, State<A::Conn>>,
    egress: EgressCtx<'_, 'd, '_>,
) -> bool
where
    A: Application<'d>,
{
    match catch_unwind(AssertUnwindSafe(|| {
        A::Hooks::teardown(app.as_mut(), slot, egress)
    })) {
        Ok(()) => true,
        Err(payload) => {
            abort_on_drop_panic(payload);
            false
        }
    }
}

#[pinned_drop]
impl<'pool, 'd, const ID: u8, A, E> PinnedDrop for Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        let capacity = this.pool.capacity();
        let mut failed = false;
        for idx in capacity.slots() {
            let Some(slot) = this.pool.get_mut(idx) else {
                continue;
            };
            let egress = EgressCtx::for_slot(this.aux, this.egress_arena, idx);
            failed |= !teardown_slot(this.app.as_mut(), slot, egress);
        }
        if failed {
            abort();
        }
    }
}

impl<'pool, 'd, const ID: u8, A, E> Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    pub fn open_in(
        app: A,
        config: Config<E::Transport>,
        hash_builder: hash::State,
        egress_storage: &'pool link::egress::storage::Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        E::Transport: ListenerTransport,
        <A::Wire as Wire>::InitConfig<'d>: Default,
    {
        Self::open_in_with_wire(
            app,
            config,
            <A::Wire as Wire>::InitConfig::<'d>::default(),
            hash_builder,
            egress_storage,
            driver,
        )
    }

    pub fn open_in_with_wire(
        app: A,
        config: Config<E::Transport>,
        wire_config: <A::Wire as Wire>::InitConfig<'d>,
        hash_builder: hash::State,
        egress_storage: &'pool link::egress::storage::Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        E::Transport: ListenerTransport,
    {
        let mut listener = Self::assemble(
            app,
            config,
            wire_config,
            hash_builder,
            egress_storage,
            driver,
        )?;
        listener.accept.request_rearm();
        Ok(listener)
    }

    fn assemble(
        app: A,
        config: Config<E::Transport>,
        wire_config: <A::Wire as Wire>::InitConfig<'d>,
        hash_builder: hash::State,
        egress_storage: &'pool link::egress::storage::Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        E::Transport: ListenerTransport,
    {
        let Config {
            max_connections,
            bind,
            backlog,
            mut stream,
            transport,
            egress,
        } = config;
        <E::Transport as Transport>::apply_profile_defaults(&mut stream, E::Profile::USER_TIMEOUT);
        <E::Transport as Transport>::validate_stream_config(stream)?;
        let Some(capacity) = TokenCapacity::new(max_connections).filter(|_| max_connections != 0)
        else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: listener capacity must be in 1..=2^24-1",
            ));
        };
        let Some(accept_slot) = capacity.sentinel() else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: listener capacity must be in 1..=2^24-1",
            ));
        };
        let max_retained_recv_chunks = A::max_retained_recv_chunks(max_connections)?;
        let per_ip_limit = <E::Transport as ListenerTransport>::per_ip_limit(&transport)
            .unwrap_or(E::Profile::PER_IP_CAP);
        let prepared_pool = Pool::prepare_with_recv_credit(
            capacity,
            max_retained_recv_chunks,
            A::RETAIN_RAW_RECV,
            wire_config,
            driver,
        )?;
        if egress_storage.config() != egress {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "listener egress storage does not match its egress config",
            ));
        }
        let egress_arena = Arena::with_config(
            egress_storage,
            driver.region_token_ref(),
            egress,
            max_connections,
        );
        let aux = Aux::new(max_connections);
        let idle = IdleSet::new(capacity, E::Profile::IDLE_WINDOW);
        let idle_send = IdleSet::new(
            if E::Profile::SEND_DEADLINE.is_some() {
                capacity
            } else {
                TokenCapacity::EMPTY
            },
            E::Profile::SEND_DEADLINE.unwrap_or(Duration::ZERO),
        );
        let idle_abs_age = IdleSet::new(
            if E::Profile::ABS_CONN_AGE.is_some() {
                capacity
            } else {
                TokenCapacity::EMPTY
            },
            E::Profile::ABS_CONN_AGE.unwrap_or(Duration::ZERO),
        );
        let dirty = PendingQueue::with_capacity(max_connections);
        let mut route = Route::reserve_transaction(driver)?;
        let (listener_fd, bound_addr) = <E::Transport as ListenerTransport>::bind_listener_slot(
            route.driver(),
            &bind,
            backlog,
            &transport,
        )?;
        let accept = Accept::new(listener_fd, accept_slot, stream, per_ip_limit, hash_builder);
        let pool = prepared_pool.bind(OutboundReservation::empty());
        let route = route.commit();
        Ok(Self {
            route,
            pool,
            egress_arena,
            accept,
            bound_addr,
            app,
            aux,
            idle,
            idle_send,
            idle_abs_age,
            dirty,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr>
    where
        E::Transport: 'static,
    {
        Ok(self.bound_addr)
    }

    pub fn handler(&self) -> &A {
        &self.app
    }

    pub fn capacity(&self) -> usize {
        self.pool.capacity().get()
    }

    pub fn handler_mut(self: Pin<&mut Self>) -> Pin<&mut A> {
        self.project().app
    }

    pub fn wire_runtime(self: Pin<&mut Self>) -> &mut <A::Wire as Wire>::RuntimeContext<'d> {
        self.project().pool.wire_runtime()
    }

    pub fn set_close_after(self: Pin<&mut Self>, conn_id: Token) {
        let this = self.project();
        if let Some((_, slot)) = this.pool.by_target_mut(conn_id) {
            slot.set_close_after();
        }
    }

    pub fn has_pending_egress(&self, conn_id: Token) -> bool {
        self.pool.by_target(conn_id).is_some_and(|(idx, slot)| {
            slot.is_send_inflight()
                || slot.state.send.consumed_plain < slot.state.send.total_plain
                || slot.holds_plain()
                || self.egress_arena.bytes(idx.raw() as usize) != 0
        })
    }

    pub fn mark_send(
        &self,
        region: &mut o3::cell::RegionToken<'d>,
        conn_id: Token,
        bytes: Shared,
    ) -> bool {
        let Some((idx, slot)) = self.pool.by_target(conn_id) else {
            return false;
        };
        let staged = self
            .egress_arena
            .try_enqueue(region, idx.raw() as usize, bytes.into())
            .is_ok();
        self.dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
        staged
    }

    pub fn close(&self, conn_id: Token) {
        let Some((idx, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        self.dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
    }

    pub fn activate(mut self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().resume_recv(target, driver);
        let idx = {
            let mut fields = self.as_mut().project();
            let Some((idx, slot)) = fields.pool.by_target_mut(target) else {
                return;
            };
            let egress = EgressCtx::for_slot(fields.aux, fields.egress_arena, idx);
            A::Hooks::activate(fields.app.as_mut(), slot, egress, driver);
            idx
        };
        self.as_mut().maybe_close_inherent(idx, driver);
        self.flush_dirty(driver);
    }
}

impl<'pool, 'd, const ID: u8, A, E> Manifold<'d> for Listener<'pool, 'd, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        match ev {
            Event::Recv(token, more, e) => this.as_mut().pump_recv(token, more, e, driver),
            Event::Send(token, e) => this.as_mut().pump_send(token, e, driver),
            Event::Accept(token, more, e) => this.as_mut().accept_inherent(token, more, e, driver),
            _ => {}
        }
        let fields = this.as_mut().project();
        if fields.accept.needs_rearm() {
            fields.accept.arm(ID, driver);
        }
        fields.pool.flush_rearm(driver);
        this.flush_dirty(driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.activate(target.into_inner(), driver);
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        let capacity = {
            let fields = this.as_mut().project();
            fields.accept.stop_accept(ID, driver);
            fields.pool.capacity()
        };
        for idx in capacity.slots() {
            Self::close_inherent(this.as_mut(), idx, driver);
        }
        let fields = this.as_mut().project();
        let mut quiesce = driver.quiesce_batch();
        if let Some(target) = fields.accept.quiesce_target(ID) {
            quiesce.cancel(target);
        }
        fields
            .pool
            .for_each_io_target(|target| quiesce.cancel(target));
        let outcome = quiesce.finish();
        let poison = fields.pool.needs_route_poison() || outcome.has_targets();
        fields.route.finish(driver, poison);
    }

    fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        self.project().accept.finish(context);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        {
            let fields = this.as_mut().project();
            if fields.accept.needs_rearm() {
                fields.accept.arm(ID, driver);
            }
            fields.pool.flush_rearm(driver);
        }
        this.as_mut().flush_dirty(driver);
        let now = driver.turn_now();
        this.as_mut().drain_idle(now, driver, |s| s.project().idle);
        if E::Profile::SEND_DEADLINE.is_some() {
            this.as_mut()
                .drain_idle(now, driver, |s| s.project().idle_send);
        }
        if E::Profile::ABS_CONN_AGE.is_some() {
            this.as_mut()
                .drain_idle(now, driver, |s| s.project().idle_abs_age);
        }
        this.flush_dirty(driver);
    }

    fn idle(self: Pin<&Self>, _region: &o3::cell::RegionToken<'d>) -> Idle {
        let listener = self;
        let this = listener.project_ref();
        if !this.dirty.is_empty()
            || this.accept.needs_rearm()
            || (E::Profile::HYBRID_PARK && this.pool.pending_recv_rearm())
        {
            return Idle::Busy;
        }
        let deadline = [
            this.idle.earliest(),
            this.idle_send.earliest(),
            this.idle_abs_age.earliest(),
        ]
        .into_iter()
        .flatten()
        .min();
        Idle::Park(deadline)
    }
}
