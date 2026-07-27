pub mod application;
pub mod config;
pub mod egress;
mod idle;
mod raw;
pub mod recv;
mod send;
pub mod state;

use std::io;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};

use dope_core::driver::control::ContextControl;
use std::pin::Pin;
use std::process::abort;
use std::time::Duration;

use application::Application;
use application::ApplicationPhase;
use config::Config;
use egress::{EgressPhase, SlotFlow};
use idle::{IdlePhase, IdleSet};
use raw::accept::{Accept, AcceptPhase};
use send::SendPhase;
use state::{Aux, State};

use crate::DriverContext;
use crate::hash;
use crate::manifold::Manifold;
use crate::manifold::env::{Bundle, Env};
use crate::manifold::typed::TypedToken;
use crate::panic::abort_on_drop_panic;
use crate::runtime::dispatcher::Idle;
use crate::runtime::profile::Balanced;
use crate::runtime::profile::RuntimeProfile;
use dope_core::driver::OutboundReservation;
use dope_core::driver::route::Route;
use dope_core::driver::token::{SLOT_MASK, SlotIndex, Token};
use dope_core::io::Event;
use dope_core::io::EventKind;
use dope_net::Transport;
use dope_net::link::egress::arena::Arena;
use dope_net::link::raw::pool::Pool;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PendingQueue, SendBuffer, Slot};
use dope_net::tcp::Tcp;
use dope_net::wire::Wire;
use o3::buffer::Shared;
use pin_project::pin_project;
use pin_project::pinned_drop;

#[pin_project(PinnedDrop, !Unpin)]
pub struct Listener<'d, const ID: u8, A, E = Bundle<Tcp, <A as Application<'d>>::Wire, Balanced>>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    route: Route<'d, ID>,
    pool: Pool<'d, ID, E::Transport, A::Wire, State<A::Conn>>,
    egress_arena: Arena<SendBuffer>,
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
    aux: &mut Aux,
) -> bool
where
    A: Application<'d>,
{
    match catch_unwind(AssertUnwindSafe(|| app.as_mut().teardown(slot, aux))) {
        Ok(()) => true,
        Err(payload) => {
            abort_on_drop_panic(payload);
            false
        }
    }
}

#[pinned_drop]
impl<'d, const ID: u8, A, E> PinnedDrop for Listener<'d, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        let capacity = this.pool.capacity();
        let mut failed = false;
        for raw in 0..capacity as u32 {
            let Some(slot) = this.pool.get_mut(SlotIndex::new(raw)) else {
                continue;
            };
            failed |= !teardown_slot(this.app.as_mut(), slot, this.aux);
        }
        if failed {
            abort();
        }
    }
}

impl<'d, const ID: u8, A, E> Listener<'d, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    pub fn open_in(
        app: A,
        config: Config<E::Transport>,
        hash_builder: hash::State,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <A::Wire as Wire>::InitConfig: Default,
    {
        Self::open_in_with_wire(
            app,
            config,
            <A::Wire as Wire>::InitConfig::default(),
            hash_builder,
            driver,
        )
    }

    pub fn open_in_with_wire(
        app: A,
        config: Config<E::Transport>,
        wire_config: <A::Wire as Wire>::InitConfig,
        hash_builder: hash::State,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let mut listener = Self::assemble(app, config, wire_config, hash_builder, driver)?;
        listener.accept.request_rearm();
        Ok(listener)
    }

    fn assemble(
        app: A,
        config: Config<E::Transport>,
        wire_config: <A::Wire as Wire>::InitConfig,
        hash_builder: hash::State,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let Config {
            max_connections,
            bind,
            backlog,
            mut stream,
            transport,
            egress,
        } = config;
        <E::Transport as Transport>::apply_profile_defaults(&mut stream, E::Profile::USER_TIMEOUT);
        assert!(
            max_connections > 0 && max_connections <= SLOT_MASK as usize + 1,
            "max_connections must be in 1..=1<<24"
        );
        let route = Route::reserve(driver)?;
        let (listener_fd, bound_addr) =
            <E::Transport as Transport>::bind_listener_slot(driver, &bind, backlog, &transport)?;
        let per_ip_limit =
            <E::Transport as Transport>::per_ip_limit(&transport).unwrap_or(E::Profile::PER_IP_CAP);
        Ok(Self {
            route,
            pool: Pool::new(
                max_connections,
                A::max_retained_recv_chunks(max_connections)?,
                OutboundReservation::empty(),
                wire_config,
                driver,
            )?,
            egress_arena: Arena::with_config(egress, max_connections),
            accept: Accept::new(
                listener_fd,
                max_connections as u32,
                stream,
                per_ip_limit,
                hash_builder,
            ),
            bound_addr,
            app,
            aux: Aux::new(max_connections),
            idle: IdleSet::new(max_connections, E::Profile::IDLE_WINDOW),
            idle_send: IdleSet::new(
                if E::Profile::SEND_DEADLINE.is_some() {
                    max_connections
                } else {
                    0
                },
                E::Profile::SEND_DEADLINE.unwrap_or(Duration::ZERO),
            ),
            idle_abs_age: IdleSet::new(
                if E::Profile::ABS_CONN_AGE.is_some() {
                    max_connections
                } else {
                    0
                },
                E::Profile::ABS_CONN_AGE.unwrap_or(Duration::ZERO),
            ),
            dirty: PendingQueue::with_capacity(max_connections),
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

    pub fn handler_mut(self: Pin<&mut Self>) -> Pin<&mut A> {
        self.project().app
    }

    pub fn wire_runtime(self: Pin<&mut Self>) -> &mut <A::Wire as Wire>::RuntimeContext {
        self.project().pool.wire_runtime()
    }

    pub fn set_close_after(self: Pin<&mut Self>, conn_id: Token) {
        let this = self.project();
        if let Some((_, slot)) = this.pool.by_target_mut(conn_id) {
            slot.core.set_close_after();
        }
    }

    pub fn has_pending_egress(&self, conn_id: Token) -> bool {
        self.pool
            .by_target(conn_id)
            .is_some_and(|(_, slot)| slot.owes_egress())
    }

    pub fn mark_send(&self, conn_id: Token, bytes: Shared) -> bool {
        let Some((idx, slot)) = self.pool.by_target(conn_id) else {
            return false;
        };
        let staged = slot.state.deferred.stage(bytes, false);
        self.dirty.mark(idx, &slot.state.pending, PEND_EGRESS);
        staged
    }

    pub fn close(&self, conn_id: Token) {
        let Some((idx, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        self.dirty.mark(idx, &slot.state.pending, PEND_CLOSE);
    }
}

impl<'d, const ID: u8, A, E> Manifold<'d> for Listener<'d, ID, A, E>
where
    A: Application<'d>,
    E: Env<Wire = A::Wire>,
{
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        match ev.into_kind() {
            EventKind::Recv(token, more, e) => this.as_mut().pump_recv(token, more, e, driver),
            EventKind::Send(token, e) => this.as_mut().pump_send(token, e, driver),
            EventKind::Accept(token, more, e) => {
                this.as_mut().accept_inherent(token, more, e, driver)
            }
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
        let mut this = self;
        let conn_id = target.into_inner();
        let idx = {
            let mut fields = this.as_mut().project();
            let Some((idx, slot)) = fields.pool.by_target_mut(conn_id) else {
                return;
            };
            fields.app.as_mut().activate(slot, fields.aux, driver);
            idx
        };
        this.as_mut().maybe_close_inherent(idx, driver);
        this.flush_dirty(driver);
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let mut this = self;
        let cap = {
            let fields = this.as_mut().project();
            fields.accept.stop_accept(ID, driver);
            fields.pool.capacity() as u32
        };
        for raw in 0..cap {
            Self::close_inherent(this.as_mut(), SlotIndex::new(raw), driver);
        }
        let fields = this.as_mut().project();
        let mut targets = Vec::new();
        fields.accept.append_target(ID, &mut targets);
        fields.pool.append_io_targets(&mut targets);
        let poison = fields.pool.needs_route_poison() || !targets.is_empty();
        if !targets.is_empty() {
            driver.quiesce(&targets);
        }
        fields.route.finish(driver, poison);
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

    fn idle(self: Pin<&Self>) -> Idle {
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
