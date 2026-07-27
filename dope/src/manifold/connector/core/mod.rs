mod close;
mod connect;
mod recv;
mod send;
mod source;

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;

use self::close::ClosePhase;
use self::connect::ConnectPhase;
use self::recv::RecvPhase;
use self::send::SendPhase;
use self::source::SourcePhase;
use super::app::ConnApp;
use super::session::{Session, SessionApp};
use super::source::{DialKey, Dialer};
use super::state::State;
use crate::DriverContext;
use crate::manifold::Manifold;
use crate::manifold::env::Env;
use crate::manifold::timer::{Ticket, Timer};
use crate::manifold::typed::TypedToken;
use crate::runtime::dispatcher::Idle;
use crate::runtime::profile::RuntimeProfile;
use dope_core::driver::control::ContextControl;
use dope_core::driver::ready::{ReadyKey, ReadySlot};
use dope_core::driver::route::Route;
use dope_core::driver::token::{Epoch, SlotIndex, Token};
use dope_core::io::Event;
use dope_core::io::EventKind;
use dope_net::Transport;
use dope_net::link::egress::arena::Arena;
use dope_net::link::egress::config::Config;
use dope_net::link::raw::pool::Pool;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PendingQueue};
use dope_net::wire::Wire;
use pin_project::pin_project;

type ConnPool<'d, const ID: u8, T, W, C, S> = Pool<'d, ID, T, W, State<C, S>>;

#[pin_project(!Unpin)]
pub struct Core<'d, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    route: Route<'d, ID>,
    pub(super) pool: ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
    egress_arena: Arena<A::Send>,
    pub(super) app: A,
    pub(super) upstreams: S,
    dirty: PendingQueue,
    backoff_timer: Option<Ticket>,
    liveness_timer: Option<Ticket>,
    timer: Timer<'d, 0>,
    #[pin]
    backoff_slot: ReadySlot<'d>,
    draining: bool,
    _e: PhantomData<E>,
}

impl<'d, const ID: u8, A, S, E> Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    pub fn with_app(
        app: A,
        upstreams: S,
        max_connections: usize,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <A::Wire as Wire>::InitConfig: Default,
    {
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            Config::default(),
            <A::Wire as Wire>::InitConfig::default(),
            driver,
        )
    }

    pub fn with_app_wire_config(
        app: A,
        upstreams: S,
        max_connections: usize,
        wire_config: <A::Wire as Wire>::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            Config::default(),
            wire_config,
            driver,
        )
    }

    pub fn with_app_config(
        app: A,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <A::Wire as Wire>::InitConfig: Default,
    {
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            egress_config,
            <A::Wire as Wire>::InitConfig::default(),
            driver,
        )
    }

    pub fn with_app_configs(
        app: A,
        mut upstreams: S,
        max_connections: usize,
        egress_config: Config,
        wire_config: <A::Wire as Wire>::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let route = Route::reserve(driver)?;
        let reservation = driver.reserve_outbound(max_connections as u32)?;
        let backoff_sentinel =
            Token::new(ID, SlotIndex::new(max_connections as u32), Epoch::INITIAL);
        let backoff_slot = driver.driver_ref().make_ready_slot(backoff_sentinel)?;
        upstreams.resize(max_connections);
        let pool = Pool::new(
            max_connections,
            A::max_retained_recv_chunks(max_connections)?,
            reservation,
            wire_config,
            driver,
        )?;
        Ok(Self {
            route,
            pool,
            egress_arena: Arena::with_config(egress_config, max_connections),
            app,
            upstreams,
            dirty: PendingQueue::with_capacity(max_connections),
            backoff_timer: None,
            liveness_timer: None,
            timer: Timer::with_capacity(2, driver.driver_ref()),
            backoff_slot,
            draining: false,
            _e: PhantomData,
        })
    }

    fn backoff_key(self: Pin<&Self>) -> ReadyKey<'d> {
        self.project_ref().backoff_slot.key()
    }

    pub fn dial(
        mut self: Pin<&mut Self>,
        addr: <E::Transport as Transport>::Addr,
    ) -> Option<DialKey> {
        let driver = self.as_ref().get_ref().route.driver();
        let this = self.as_mut().project();
        let key = this
            .upstreams
            .dial(addr, <E::Transport as Transport>::StreamConfig::default())?;
        driver.activate_ready(self.as_ref().backoff_key());
        Some(key)
    }

    pub fn flush(&self, conn_id: Token) {
        let Some((slot_index, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        self.dirty
            .mark(slot_index, &slot.state.pending, PEND_EGRESS);
    }

    pub fn revive_upstreams(self: Pin<&mut Self>) {
        let ready = self.as_ref().backoff_key();
        let driver = self.as_ref().get_ref().route.driver();
        let this = self.project();
        this.upstreams.revive();
        driver.activate_ready(ready);
    }

    pub fn close(&self, conn_id: Token) {
        let Some((slot_index, slot)) = self.pool.by_target(conn_id) else {
            return;
        };
        self.dirty.mark(slot_index, &slot.state.pending, PEND_CLOSE);
    }

    pub fn state(&self, conn_id: Token) -> Option<&State<A::Conn, A::Send>> {
        let (_, slot) = self.pool.by_target(conn_id)?;
        Some(&slot.state)
    }

    pub fn handler(&self) -> &A {
        &self.app
    }

    pub fn wire_runtime(self: Pin<&mut Self>) -> &mut <A::Wire as Wire>::RuntimeContext {
        self.project().pool.wire_runtime()
    }

    fn rouse(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().flush_cancellations();
        self.as_mut().poll_source(driver);
        self.as_mut().poll_liveness(driver);
        self.flush_dirty(driver);
    }
}

impl<'d, const ID: u8, N, S, E> Core<'d, ID, SessionApp<'d, N, E::Wire>, S, E>
where
    N: Session<'d>,
    S: Dialer<E::Transport>,
    E: Env,
    E::Transport: Transport,
{
    pub fn new(
        session: N,
        upstreams: S,
        max_connections: usize,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <E::Wire as Wire>::InitConfig: Default,
    {
        Self::new_with_egress(
            session,
            upstreams,
            max_connections,
            Config::default(),
            driver,
        )
    }

    pub fn new_with_egress(
        session: N,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <E::Wire as Wire>::InitConfig: Default,
    {
        Self::new_with_configs(
            session,
            upstreams,
            max_connections,
            egress_config,
            <E::Wire as Wire>::InitConfig::default(),
            driver,
        )
    }

    pub fn new_with_wire_config(
        session: N,
        upstreams: S,
        max_connections: usize,
        wire_config: <E::Wire as Wire>::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Self::new_with_configs(
            session,
            upstreams,
            max_connections,
            Config::default(),
            wire_config,
            driver,
        )
    }

    pub fn new_with_configs(
        session: N,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        wire_config: <E::Wire as Wire>::InitConfig,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let app = SessionApp {
            session,
            wire: PhantomData,
        };
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            egress_config,
            wire_config,
            driver,
        )
    }

    pub fn session(&self) -> &N {
        &self.app.session
    }

    pub fn session_mut(self: Pin<&mut Self>) -> &mut N {
        &mut self.project().app.session
    }
}

impl<'d, const ID: u8, A, S, E> Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    pub fn dispatch(mut self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        match ev.into_kind() {
            EventKind::Recv(token, more, e) => self.as_mut().handle_recv(token, more, e, driver),
            EventKind::Send(token, e) => self.as_mut().handle_send(token, e, driver),
            EventKind::Socket(token, e) => self.as_mut().socket(token, e, driver),
            EventKind::Connect(token, e) => self.as_mut().connect(token, e, driver),
            _ => {}
        }
        self.as_mut().project().pool.flush_rearm(driver);
        self.as_mut().flush_dirty(driver);
    }

    pub fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        {
            let this = self.as_mut().project();
            this.timer.expire(driver.turn_now());
            this.app.pre_park();
        }
        self.rouse(driver);
    }

    pub fn activate(mut self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().apply_requests(target, driver);
        self.rouse(driver);
    }

    pub fn idle(self: Pin<&Self>) -> Idle {
        let this = self.project_ref();
        if !this.dirty.is_empty() || (E::Profile::HYBRID_PARK && this.pool.pending_recv_rearm()) {
            return Idle::Busy;
        }
        Idle::Park(this.timer.earliest()).reduce(this.app.idle())
    }

    pub fn shutdown_all(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        {
            let this = self.as_mut().project();
            *this.draining = true;
            if let Some(t) = this.backoff_timer.take() {
                this.timer.cancel(t);
            }
        }
        let cap = self.as_ref().project_ref().pool.capacity() as u32;
        for raw in 0..cap {
            self.as_mut().close_slot(SlotIndex::new(raw), driver);
        }
        self.as_mut().flush_dirty(driver);
        let fields = self.project();
        let mut targets = Vec::new();
        fields.pool.append_io_targets(&mut targets);
        fields.pool.append_outbound_targets(&mut targets);
        let poison = fields.pool.needs_route_poison() || !targets.is_empty();
        if !targets.is_empty() {
            driver.quiesce(&targets);
        }
        fields.route.finish(driver, poison);
    }
}

impl<'d, const ID: u8, A, S, E> Manifold<'d> for Core<'d, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    const ID: u8 = ID;

    fn dispatch(self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        self.dispatch(ev, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.pre_park(driver);
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.activate(target.into_inner(), driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        self.idle()
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.shutdown_all(driver);
    }
}
