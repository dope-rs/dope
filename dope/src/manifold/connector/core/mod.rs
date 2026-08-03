mod close;
mod connect;
mod recv;
mod send;
mod source;

use std::io::{self, Error, ErrorKind};
use std::marker::PhantomData;
use std::pin::Pin;

use dope_core::driver::ready::{ReadyKey, ReadySlot};
use dope_core::driver::route::Route;
use dope_core::driver::token::{Epoch, Token, TokenCapacity};
use dope_core::io::Event;
use dope_net::Transport;
use dope_net::link::egress::arena::Arena;
use dope_net::link::egress::config::Config;
use dope_net::link::egress::storage::Storage;
use dope_net::link::raw::pool::Pool;
use dope_net::link::raw::pool::outbound::OutboundPool;
use dope_net::link::slot::{PEND_CLOSE, PEND_EGRESS, PendingQueue};
use dope_net::wire::Wire;
use o3::cell::RegionToken;
use pin_project::pin_project;

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
use crate::driver::timer::Registration;
use crate::manifold::Manifold;
use crate::manifold::env::Env;
use crate::manifold::typed::TypedToken;
use crate::runtime::dispatcher::{FinishContext, Idle};
use crate::runtime::profile::RuntimeProfile;

type ConnPool<'d, const ID: u8, T, W, C, S> = Pool<'d, ID, T, W, State<C, S>>;

#[pin_project(!Unpin)]
pub struct Core<'pool, 'd, const ID: u8, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    route: Route<'d, ID>,
    pub(super) pool: ConnPool<'d, ID, E::Transport, A::Wire, A::Conn, A::Send>,
    egress_arena: Arena<'d, 'pool, A::Send>,
    pub(super) app: A,
    pub(super) upstreams: S,
    dirty: PendingQueue,
    #[pin]
    backoff_timer: Registration<'d, 'd>,
    #[pin]
    liveness_timer: Registration<'d, 'd>,
    #[pin]
    backoff_slot: ReadySlot<'d>,
    draining: bool,
    _e: PhantomData<E>,
}

impl<'pool, 'd, const ID: u8, A, S, E> Core<'pool, 'd, ID, A, S, E>
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
        egress_storage: &'pool Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <A::Wire as Wire>::InitConfig<'d>: Default,
    {
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            Config::default(),
            <A::Wire as Wire>::InitConfig::<'d>::default(),
            egress_storage,
            driver,
        )
    }

    pub fn with_app_wire_config(
        app: A,
        upstreams: S,
        max_connections: usize,
        wire_config: <A::Wire as Wire>::InitConfig<'d>,
        egress_storage: &'pool Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            Config::default(),
            wire_config,
            egress_storage,
            driver,
        )
    }

    pub fn with_app_config(
        app: A,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        egress_storage: &'pool Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <A::Wire as Wire>::InitConfig<'d>: Default,
    {
        Self::with_app_configs(
            app,
            upstreams,
            max_connections,
            egress_config,
            <A::Wire as Wire>::InitConfig::<'d>::default(),
            egress_storage,
            driver,
        )
    }

    pub fn with_app_configs(
        app: A,
        mut upstreams: S,
        max_connections: usize,
        egress_config: Config,
        wire_config: <A::Wire as Wire>::InitConfig<'d>,
        egress_storage: &'pool Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        let Some(capacity) = TokenCapacity::new(max_connections).filter(|_| max_connections != 0)
        else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: connector capacity must be in 1..=2^24-1",
            ));
        };
        let Some(backoff_index) = capacity.sentinel() else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dope: connector capacity must be in 1..=2^24-1",
            ));
        };
        let max_retained_recv_chunks = A::max_retained_recv_chunks(max_connections)?;
        upstreams.resize(max_connections);
        let prepared_pool = Pool::prepare_with_recv_credit(
            capacity,
            max_retained_recv_chunks,
            A::RETAIN_RAW_RECV,
            wire_config,
            driver,
        )?;
        if egress_storage.config() != egress_config {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "connector egress storage does not match its egress config",
            ));
        }
        let egress_arena = Arena::with_config(
            egress_storage,
            driver.region_token_ref(),
            egress_config,
            max_connections,
        );
        let dirty = PendingQueue::with_capacity(max_connections);
        let timer = driver.timer();
        let backoff_sentinel = Token::new(ID, backoff_index, Epoch::INITIAL);
        let backoff_slot = driver.driver_ref().make_ready_slot(backoff_sentinel)?;
        let mut route = Route::reserve_transaction(driver)?;
        let reservation = route.driver().reserve_outbound(capacity.get() as u32)?;
        let pool = prepared_pool.bind(reservation);
        let route = route.commit();
        Ok(Self {
            route,
            pool,
            egress_arena,
            app,
            upstreams,
            dirty,
            backoff_timer: Registration::new(timer),
            liveness_timer: Registration::new(timer),
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

    pub fn wire_runtime(self: Pin<&mut Self>) -> &mut <A::Wire as Wire>::RuntimeContext<'d> {
        self.project().pool.wire_runtime()
    }

    fn rouse(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().flush_cancellations();
        self.as_mut().poll_source(driver);
        self.as_mut().poll_liveness(driver);
        self.flush_dirty(driver);
    }
}

impl<'d, const ID: u8, N, S, E> Core<'d, 'd, ID, SessionApp<'d, N, E::Wire>, S, E>
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
        egress_storage: &'d Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <E::Wire as Wire>::InitConfig<'d>: Default,
    {
        Self::new_with_egress(
            session,
            upstreams,
            max_connections,
            Config::default(),
            egress_storage,
            driver,
        )
    }

    pub fn new_with_egress(
        session: N,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        egress_storage: &'d Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self>
    where
        <E::Wire as Wire>::InitConfig<'d>: Default,
    {
        Self::new_with_configs(
            session,
            upstreams,
            max_connections,
            egress_config,
            <E::Wire as Wire>::InitConfig::<'d>::default(),
            egress_storage,
            driver,
        )
    }

    pub fn new_with_wire_config(
        session: N,
        upstreams: S,
        max_connections: usize,
        wire_config: <E::Wire as Wire>::InitConfig<'d>,
        egress_storage: &'d Storage,
        driver: &mut DriverContext<'_, 'd>,
    ) -> io::Result<Self> {
        Self::new_with_configs(
            session,
            upstreams,
            max_connections,
            Config::default(),
            wire_config,
            egress_storage,
            driver,
        )
    }

    pub fn new_with_configs(
        session: N,
        upstreams: S,
        max_connections: usize,
        egress_config: Config,
        wire_config: <E::Wire as Wire>::InitConfig<'d>,
        egress_storage: &'d Storage,
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
            egress_storage,
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

impl<'pool, 'd, const ID: u8, A, S, E> Core<'pool, 'd, ID, A, S, E>
where
    A: ConnApp<'d>,
    S: Dialer<E::Transport>,
    E: Env<Wire = A::Wire>,
    E::Transport: Transport,
{
    pub fn dispatch(mut self: Pin<&mut Self>, ev: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        match ev {
            Event::Recv(token, more, e) => self.as_mut().handle_recv(token, more, e, driver),
            Event::Send(token, e) => self.as_mut().handle_send(token, e, driver),
            Event::Socket(token, e) => self.as_mut().socket(token, e, driver),
            Event::Connect(token, e) => self.as_mut().connect(token, e, driver),
            _ => {}
        }
        self.as_mut().project().pool.flush_rearm(driver);
        self.as_mut().flush_dirty(driver);
    }

    pub fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        {
            let this = self.as_mut().project();
            this.app.pre_park(driver.region_token());
        }
        self.rouse(driver);
    }

    pub fn activate(mut self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        self.as_mut().resume_recv(target, driver);
        self.as_mut().apply_requests(target, driver);
        self.rouse(driver);
    }

    pub fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        let this = self.project_ref();
        if !this.dirty.is_empty() || (E::Profile::HYBRID_PARK && this.pool.pending_recv_rearm()) {
            return Idle::Busy;
        }
        this.app.idle(region)
    }

    pub fn shutdown_all(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        {
            let this = self.as_mut().project();
            *this.draining = true;
            this.backoff_timer.as_ref().cancel();
            this.liveness_timer.as_ref().cancel();
        }
        let capacity = self.as_ref().project_ref().pool.capacity();
        for idx in capacity.slots() {
            self.as_mut().close_slot(idx, driver);
        }
        self.as_mut().flush_dirty(driver);
        let fields = self.project();
        let mut quiesce = driver.quiesce_batch();
        fields
            .pool
            .for_each_io_target(|target| quiesce.cancel(target));
        fields
            .pool
            .for_each_outbound_target(|target| quiesce.cancel(target));
        let outcome = quiesce.finish();
        let poison = fields.pool.needs_route_poison() || outcome.has_targets();
        fields.route.finish(driver, poison);
    }

    pub fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        let reservation = self.project().pool.take_outbound_reservation();
        context.retire_outbound(reservation);
    }
}

impl<'pool, 'd, const ID: u8, A, S, E> Manifold<'d> for Core<'pool, 'd, ID, A, S, E>
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

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        self.idle(region)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        self.shutdown_all(driver);
    }

    fn finish(self: Pin<&mut Self>, context: &mut FinishContext<'_, 'd>) {
        self.finish(context);
    }
}
